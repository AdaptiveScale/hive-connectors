/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.hive.source;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.ReferenceBatchSource;
import io.cdap.plugin.hive.common.HiveAuth;
import io.cdap.plugin.hive.common.HiveConfig;
import io.cdap.plugin.hive.common.KerberosAuth;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/**
 * Hive Batch Source to read records from external Hive tables.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("Hive")
@Description("Hive Batch Source to read records from external Hive tables.")
public class HiveBatchSource extends ReferenceBatchSource<WritableComparable, Object, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(HiveBatchSource.class);
  private HiveConfig config;

  public HiveBatchSource(HiveConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
//    if (config.schema != null) {
//      try {
//        pipelineConfigurer.getStageConfigurer().setOutputSchema(Schema.parseJson(config.schema));
//      } catch (Exception e) {
//        throw new IllegalArgumentException("Invalid output schema: " + e.getMessage(), e);
//      }
//    }
  }

  @Override
  public void prepareRun(BatchSourceContext batchSourceContext) throws Exception {
    // make connection to hive table
    Connection con = null;
    String authType = config.getAuthentication();
    HiveAuth auth = HiveAuth.getAuth(authType);

    // check if JDBC driver is provided
    if (!Strings.isNullOrEmpty(config.jdbcDriver)) {
      Class.forName(config.jdbcDriver);
    }

    if (Strings.isNullOrEmpty(config.url) || Strings.isNullOrEmpty(config.userName) ||
      Strings.isNullOrEmpty(config.password)) {
      throw new RuntimeException(String.format("Properties %s, %s and %s should all be defined",
                                               HiveConfig.NAME_URL, HiveConfig.NAME_USERNAME, HiveConfig.NAME_PASSWORD));
    }

    switch (auth) {
      case NONE:
        con = DriverManager.getConnection(config.url, config.userName, config.password);
        break;

      case KERBEROS:
        String hadoopConfDir = "/etc/hadoop/conf";
        String hiveHostname = config.url.replace("jdbc:hive2://", "").split(":")[0];

        URL url = HiveBatchSource.class.getClassLoader().getResource("jaas.conf");
        System.setProperty("java.security.auth.login.config", url.toExternalForm());
        Configuration configuration = new Configuration();
        configuration.addResource(new Path("file:///" + hadoopConfDir + "/core-site.xml"));
        configuration.addResource(new Path("file:///" + hadoopConfDir + "/hdfs-site.xml"));
        UserGroupInformation.setConfiguration(configuration);
        LoginContext lc = null;
        try {
          lc = KerberosAuth.kinit(config.userName, config.password);
        } catch (LoginException e) {
          throw new RuntimeException(String.format("Cannot authenticate with Kerberos with the provided username: %s " +
                                                     "and password: %s!", config.userName, config.password));
        }

        UserGroupInformation.loginUserFromSubject(lc.getSubject());
        String kerberosRealm = UserGroupInformation.getLoginUser().getUserName().split("@")[1];
        con = DriverManager
          .getConnection(config.url + "/;principal=hive/" + hiveHostname + "@" + kerberosRealm +
                           ";saslQop=auth-conf");
        break;
      case LDAP:
        assert true;

    }

    con.getCatalog();



//    LineageRecorder lineageRecorder = new LineageRecorder(batchSourceContext, config.referenceName);
//    lineageRecorder.createExternalDataset(config.getSchema());

//    context.setInput(Input.of(config.referenceName, new SourceInputFormatProvider(HCatInputFormat.class, conf)));

  }


  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    // initialized context
    super.initialize(context);

    // get schema from hive table: hCatSchema
    // If the user didn't provide any schema, convert the hive schema to CDAP schema
    // initialize HCatRecordTransformer
  }

//  @Override
//  public void transform(KeyValue<WritableComparable, HCatRecord> input, Emitter<StructuredRecord> emitter) throws Exception {
//    StructuredRecord record = hCatRecordTransformer.toRecord(input.getValue());
//    emitter.emit(record);
//  }
}
