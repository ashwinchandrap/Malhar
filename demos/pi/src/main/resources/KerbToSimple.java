
import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;

public class KerbToSimple
{
  public static void main(String[] arg) throws Exception
  {
    String remoteFs = arg[0];
    Configuration config = new Configuration(false);

    SecurityUtil.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.SIMPLE, config);
    config.setBoolean(CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY, true);
    config.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false);

    config.set(FileSystem.FS_DEFAULT_NAME_KEY, remoteFs);

    FileSystem fs = FileSystem.newInstance(new Path("/").toUri(), config);
    Path path = new Path("/user/ashwin/test.txt");
    FSDataInputStream fsis = fs.open(path);
    BufferedReader br = new BufferedReader(new InputStreamReader(fsis));
    String line;
    while ((line = br.readLine()) != null) {
      System.out.println(line);
    }
  }
}
