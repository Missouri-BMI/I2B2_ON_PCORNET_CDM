import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class PasswordUtils {

    public String getHashedPassword(String pass) {
		try {
			MessageDigest md5 = MessageDigest.getInstance("MD5");
			md5.update(pass.getBytes());
			return toHex(md5.digest());
		} catch (NoSuchAlgorithmException e) {
			System.out.println("NoSuchAlgorithm MD5!: " + e);    
		}
		return pass;
	}

    private String toHex(byte[] digest) {
		StringBuffer buf = new StringBuffer();
		for (int i = 0; i < digest.length; i++) {
			buf.append(Integer.toHexString((int) digest[i] & 0x00FF));
		}
		return buf.toString();
	}

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java PasswordUtils <password>");
            return;
        }
        PasswordUtils utils = new PasswordUtils();
		System.out.println("Plain Password: " + args[0]);
        String hashedPassword = utils.getHashedPassword(args[0]);
        System.out.println("Hashed Password: " + hashedPassword);
    }
}
