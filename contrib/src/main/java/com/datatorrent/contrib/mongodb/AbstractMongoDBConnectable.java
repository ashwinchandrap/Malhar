/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.mongodb;

import com.datatorrent.lib.db.Connectable;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import java.io.IOException;
import java.net.UnknownHostException;
import javax.validation.constraints.NotNull;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public abstract class AbstractMongoDBConnectable implements Connectable
{
  @NotNull
  protected String hostName;
  protected String dataBase;
  protected String userName;
  protected String password;
  protected transient MongoClient mongoClient;
  protected transient DB db;

  @Override
  public void connect() throws IOException
  {
    try {
      mongoClient = new MongoClient(hostName);
      db = mongoClient.getDB(dataBase);
      if (userName != null && password != null) {
        db.authenticate(userName, password.toCharArray());
      }
    }
    catch (UnknownHostException ex) {
      throw new RuntimeException("creating mongodb client", ex);
    }

  }

  @Override
  public void disconnect() throws IOException
  {
    mongoClient.close();
  }

  @Override
  public boolean connected()
  {
    try {
      mongoClient.getConnector().getDBPortPool(mongoClient.getAddress()).get().ensureOpen();
    }
    catch (Exception ex) {
      return false;
    }
    return true;
  }

  public String getHostName()
  {
    return hostName;
  }

  public void setHostName(String hostName)
  {
    this.hostName = hostName;
  }

  public String getDataBase()
  {
    return dataBase;
  }

  public void setDataBase(String dataBase)
  {
    this.dataBase = dataBase;
  }

  public String getUserName()
  {
    return userName;
  }

  public void setUserName(String userName)
  {
    this.userName = userName;
  }

  public String getPassword()
  {
    return password;
  }

  public void setPassword(String password)
  {
    this.password = password;
  }


}
