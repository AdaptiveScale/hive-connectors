package io.cdap.plugin.hive.common;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

public class KerberosAuth {

  public static LoginContext kinit(String username, String password) throws LoginException {
    LoginContext lc = new LoginContext(KerberosAuth.class.getSimpleName(), callbacks -> {
      for (Callback c : callbacks) {
        if (c instanceof NameCallback)
          ((NameCallback) c).setName(username);
        if (c instanceof PasswordCallback)
          ((PasswordCallback) c).setPassword(password.toCharArray());
      }
    });
    lc.login();
    return lc;
  }
}
