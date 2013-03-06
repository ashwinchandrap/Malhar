/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.demos.mobile;

import com.google.common.collect.Range;
import com.google.common.collect.Ranges;
import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.DAG;
import com.malhartech.lib.io.ConsoleOutputOperator;
import com.malhartech.lib.io.HttpInputOperator;
import com.malhartech.lib.io.HttpOutputOperator;
import com.malhartech.lib.io.SmtpOutputOperator;
import com.malhartech.lib.testbench.RandomEventGenerator;
import com.malhartech.lib.util.Alert;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mobile Demo Application.<p>
 */
public class ApplicationAlert implements ApplicationFactory
{
  private static final Logger LOG = LoggerFactory.getLogger(ApplicationAlert.class);
  public static final String P_phoneRange = com.malhartech.demos.mobile.Application.class.getName() + ".phoneRange";
  private String ajaxServerAddr = null;
  private Range<Integer> phoneRange = Ranges.closed(9900000, 9999999);

  private void configure(Configuration conf)
  {

    this.ajaxServerAddr = System.getenv("MALHAR_AJAXSERVER_ADDRESS");
    LOG.debug(String.format("\n******************* Server address was %s", this.ajaxServerAddr));

    if (LAUNCHMODE_YARN.equals(conf.get(DAG.STRAM_LAUNCH_MODE))) {
      // settings only affect distributed mode
      conf.setIfUnset(DAG.STRAM_CONTAINER_MEMORY_MB.name(), "2048");
      conf.setIfUnset(DAG.STRAM_MASTER_MEMORY_MB.name(), "1024");
      conf.setIfUnset(DAG.STRAM_MAX_CONTAINERS.name(), "1");
    }
    else if (LAUNCHMODE_LOCAL.equals(conf.get(DAG.STRAM_LAUNCH_MODE))) {
    }

    String phoneRange = conf.get(P_phoneRange, null);
    if (phoneRange != null) {
      String[] tokens = phoneRange.split("-");
      if (tokens.length != 2) {
        throw new IllegalArgumentException("Invalid range: " + phoneRange);
      }
      this.phoneRange = Ranges.closed(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1]));
    }
    System.out.println("Phone range: " + this.phoneRange);
  }


  @Override
  public DAG getApplication(Configuration conf)
  {

    DAG dag = new DAG(conf);
    dag.setAttribute(DAG.STRAM_APPNAME, "MobileAlertApplication");
    configure(conf);

    RandomEventGenerator phones = dag.addOperator("phonegen", RandomEventGenerator.class);
    phones.setMinvalue(this.phoneRange.lowerEndpoint());
    phones.setMaxvalue(this.phoneRange.upperEndpoint());
    phones.setTuplesBlast(100000);
    phones.setTuplesBlastIntervalMillis(5);

    PhoneMovementGenerator movementgen = dag.addOperator("pmove", PhoneMovementGenerator.class);
    movementgen.setRange(20);
    movementgen.setThreshold(80);

    Alert alertOper = dag.addOperator("palert", Alert.class);
    alertOper.setAlertFrequency(10000);

    dag.addStream("phonedata", phones.integer_data, movementgen.data);

    if (this.ajaxServerAddr != null) {
      HttpOutputOperator<Object> httpOut = dag.addOperator("phoneLocationQueryResult", new HttpOutputOperator<Object>());
      httpOut.setResourceURL(URI.create("http://" + this.ajaxServerAddr + "/channel/mobile/phoneLocationQueryResult"));

      HttpInputOperator phoneLocationQuery = dag.addOperator("phoneLocationQuery", HttpInputOperator.class);
      URI u = URI.create("http://" + ajaxServerAddr + "/channel/mobile/phoneLocationQuery");
      phoneLocationQuery.setUrl(u);
      dag.addStream("query", phoneLocationQuery.outputPort, movementgen.locationQuery);

      dag.addStream("httpdata", movementgen.locationQueryResult, httpOut.input, alertOper.in);
    }
    else { // If no ajax, need to do phone seeding
      movementgen.phone_register.put("q3", 9996101);
      movementgen.phone_register.put("q1", 9994995);

      ConsoleOutputOperator console = dag.addOperator("phoneLocationQueryResult", new ConsoleOutputOperator());
      console.setStringFormat("result: %s");
      dag.addStream("consoledata", movementgen.locationQueryResult, console.input, alertOper.in);
    }

    SmtpOutputOperator mailOper = dag.addOperator("mail", new SmtpOutputOperator());
    mailOper.setFrom("jenkins@malhar-inc.com");
    mailOper.addRecipient(SmtpOutputOperator.RecipientType.TO, "jenkins@malhar-inc.com");
    mailOper.setContent("Phone Location: {}\nThis is an auto-generated message. Do not reply.");
    mailOper.setSubject("Update New Location");
    mailOper.setSmtpHost("secure.emailsrvr.com");
    mailOper.setSmtpPort(465);
    mailOper.setSmtpUserName("jenkins@malhar-inc.com");
    mailOper.setSmtpPassword("Testing1");
    mailOper.setUseSsl(true);

    dag.addStream("alert_mail", alertOper.alert1, mailOper.input);
    return dag;
  }
}