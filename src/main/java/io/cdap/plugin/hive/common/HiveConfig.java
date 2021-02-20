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

package io.cdap.plugin.hive.common;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.ReferencePluginConfig;

import javax.annotation.Nullable;

/**
 * Holds configuration necessary for {@link io.cdap.plugin.hive.source.HiveBatchSource} and
 * {@link io.cdap.plugin.hive.sink.HiveBatchSink}
 */
public class HiveConfig extends ReferencePluginConfig {

  // properties
  public static final String NAME_AUTHENTICATION = "authentication";
  public static final String NAME_USERNAME = "userName";
  public static final String NAME_PASSWORD = "password";
  public static final String NAME_URL = "url";
  public static final String NAME_JDBC_DRIVER = "jdbcDriver";
  public static final String NAME_METASTORE_URL = "metastoreUrl";
  public static final String NAME_METASTORE_DRIVER = "metastoreDriver";
  public static final String NAME_METASTORE_USERNAME = "metastoreUsername";
  public static final String NAME_METASTORE_PASSWORD = "metastorePassword";
  public static final String NAME_PRINCIPAL_NAME = "principalName";
  public static final String NAME_IMPERSONATION_NAME = "impersonationName";
  public static final String NAME_KEYTAB_FILE = "keytabFile";
  public static final String NAME_NAME_NODE_URL = "nameNodeUrl";
  public static final String NAME_STAGING_TYPE = "stagingType";
  public static final String NAME_STAGING_LOCATION = "stagingLocation";
  public static final String NAME_SCHEMA = "schema";


  // description
  public static final String DESC_AUTHENTICATION = "Authentication to Hive. Supports Kerberos, LDAP or None.";
  public static final String DESC_USERNAME = "Username to connect to Hive name node.";
  public static final String DESC_PASSWORD = "Password to connect to Hive name node.";
  public static final String DESC_URL = "URL to connect to Hive. For JDBC use URL with format " +
    "\"jdbc:hive2://<hostname>:<port>\". Fof Thrift use URL with format \"thrift://<hostname>:<port>\".";
  public static final String DESC_JDBC_DRIVER = "JDBC driver class to connect to Hive.";
  public static final String DESC_METASTORE_URL = "URL to connect to Hive Metastore. Following metastore are " +
    "supported for Hive: MysQL,Oracle, Postgres and SQL Server.";
  public static final String DESC_METASTORE_DRIVER = "Driver class to connect to Hive Metastore.";
  public static final String DESC_METASTORE_USERNAME = "Username to connect to Hive Metastore.";
  public static final String DESC_METASTORE_PASSWORD = "Password to connect to Hive Metastore.";
  public static final String DESC_PRINCIPAL_NAME = "Principal name to connect to Hive if using Kerberos.";
  public static final String DESC_IMPERSONATION_NAME = "Impersonation names can be specified by the privileged user " +
    "ot run the jobs under a different user. This is required for Kerberos.";
  public static final String DESC_KEYTAB_FILE = "Specify the keytab file if available for Kerberos.";
  public static final String DESC_NAME_NODE_URL = "URI to access HDFS. Usually this information is found in " +
    "\"hdfs-site.xml\"";
  public static final String DESC_STAGING_TYPE = "HDFS or Hive Database.";
  public static final String DESC_STAGING_LOCATION = "HDFS path or the database name.";
  public static final String DESC_SCHEMA = "Optional schema to use while reading from Hive table. If no schema is " +
    "provided then the schema of the source table will be used. Note: If you want to use a hive table which has " +
    "non-primitive types as a source then you should provide a schema here with non-primitive fields dropped else " +
    "your pipeline will fail.";

  @Name(NAME_USERNAME)
  @Description(DESC_USERNAME)
  @Macro
  public String userName;

  @Name(NAME_PASSWORD)
  @Description(DESC_PASSWORD)
  @Macro
  public String password;

  @Name(NAME_URL)
  @Description(DESC_URL)
  @Macro
  public String url; // Default: "jdbc:hive2://<hostname>:<port>"

  @Name(NAME_JDBC_DRIVER)
  @Description(DESC_JDBC_DRIVER)
  @Nullable
  @Macro
  public String jdbcDriver;

  @Name(NAME_METASTORE_URL)
  @Description(DESC_METASTORE_DRIVER)
  @Nullable
  @Macro
  public String metastoreURL;

  @Name(NAME_METASTORE_USERNAME)
  @Description(DESC_METASTORE_USERNAME)
  @Nullable
  @Macro
  public String metastoreUsername;

  @Name(NAME_METASTORE_PASSWORD)
  @Description(DESC_METASTORE_PASSWORD)
  @Nullable
  @Macro
  public String metastorePassword;

  @Name(NAME_PRINCIPAL_NAME)
  @Description(DESC_PRINCIPAL_NAME)
  @Nullable
  @Macro
  public String principalName;

  @Name(NAME_IMPERSONATION_NAME)
  @Description(DESC_IMPERSONATION_NAME)
  @Nullable
  @Macro
  public String impersonationName;

  @Name(NAME_KEYTAB_FILE)
  @Description(DESC_KEYTAB_FILE)
  @Nullable
  @Macro
  public String keytabFile;

  @Name(NAME_NAME_NODE_URL)
  @Description(DESC_NAME_NODE_URL)
  @Macro
  public String nameNodeUrl;

  @Name(NAME_STAGING_TYPE)
  @Description(DESC_STAGING_LOCATION)
  @Nullable
  @Macro
  public String stagingType;

  @Name(NAME_STAGING_LOCATION)
  @Description(DESC_STAGING_LOCATION)
  @Nullable
  @Macro
  public String stagingLocation;

  @Name(NAME_SCHEMA)
  @Description(DESC_SCHEMA)
  @Nullable
  public String schema;

  @Name(NAME_AUTHENTICATION)
  @Description(DESC_AUTHENTICATION)
  @Nullable
  @Macro
  private String authentication; // Default: None


  public HiveConfig(String referenceName) {
    super(referenceName);
  }

  public String getAuthentication() {
    return Strings.isNullOrEmpty(authentication) ? "None" : authentication;
  }

//  @Nullable
//  public Schema getSchema() {
//    try {
//      return schema == null ? null : Schema.parseJson(schema);
//    } catch (IOException e) {
//      throw new IllegalArgumentException(String.format("Unable to parse schema '%s'. Reason: %s",
//                                                       schema, e.getMessage()), e);
//    }
//  }

  public void validate(FailureCollector collector) {
    if (Strings.isNullOrEmpty(userName) && !containsMacro(NAME_USERNAME)) {
      collector.addFailure(String.format("Parameter \"%s\" is mandatory.", NAME_USERNAME),
                           "Please provide a value.").withConfigProperty(NAME_USERNAME);
    }

    if (Strings.isNullOrEmpty(password) && !containsMacro(NAME_PASSWORD)) {
      collector.addFailure(String.format("Parameter \"%s\" is mandatory.", NAME_PASSWORD), "Please provide a value.")
        .withConfigProperty(NAME_PASSWORD);
    }

    if (Strings.isNullOrEmpty(url) && !containsMacro(NAME_URL)) {
      collector.addFailure(String.format("Parameter \"%s\" is mandatory.", NAME_URL), "Please provide a value.")
        .withConfigProperty(NAME_URL);
    }

    if (Strings.isNullOrEmpty(nameNodeUrl) && containsMacro(NAME_NAME_NODE_URL)) {
      collector.addFailure(String.format("Parameter \"%s\" is mandatory.", NAME_NAME_NODE_URL),
                           "Please provide a value.").withConfigProperty(NAME_NAME_NODE_URL);
    }
  }
}
