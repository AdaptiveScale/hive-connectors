package io.cdap.plugin.hive.common;

public enum HiveAuth {
  NONE,
  KERBEROS,
  LDAP;

  public static HiveAuth getAuth(String auth) {
    if (auth.equalsIgnoreCase("none"))
      return NONE;
    else if (auth.equalsIgnoreCase("kerberos"))
      return KERBEROS;
    else
      return LDAP;
  }
}
