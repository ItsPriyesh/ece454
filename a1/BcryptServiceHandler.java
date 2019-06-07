import java.util.ArrayList;
import java.util.List;

import org.mindrot.jbcrypt.BCrypt;

public class BcryptServiceHandler implements BcryptService.Iface {

    public List<String> hashPassword(List<String> password, short logRounds) throws IllegalArgument, org.apache.thrift.TException {
        if (password.isEmpty()) {
            throw new IllegalArgument("Password list is empty!");
        } else if (logRounds < 4 || logRounds > 30) {
            throw new IllegalArgument("Log rounds out of range! Must be within [4, 30]");
        }
        try {
            List<String> ret = new ArrayList<>();
            for (String pass : password) {
                ret.add(BCrypt.hashpw(pass, BCrypt.gensalt(logRounds)));
            }
            return ret;
        } catch (Exception e) {
            throw new IllegalArgument(e.getMessage());
        }
    }

    public List<Boolean> checkPassword(List<String> password, List<String> hash) throws IllegalArgument, org.apache.thrift.TException {
        if (password.isEmpty() || hash.isEmpty()) {
            throw new IllegalArgument("Password or hash list is empty!");
        } else if (password.size() != hash.size()) {
            throw new IllegalArgument("Password and hash lists have unequal length!");
        }
        try {
            List<Boolean> ret = new ArrayList<>();
            for (int i = 0; i < password.size(); i++) {
                ret.add(BCrypt.checkpw(password.get(i), hash.get(i)));
            }
            return ret;
        } catch (Exception e) {
            throw new IllegalArgument(e.getMessage());
        }
    }
}
