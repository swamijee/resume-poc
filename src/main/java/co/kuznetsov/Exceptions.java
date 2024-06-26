package co.kuznetsov;

import org.apache.commons.lang.StringUtils;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Date;

public class Exceptions {

    public static void capture(Throwable t) {
        capture(t, null);
    }

    public static void capture(Throwable t, String message) {
        if (StringUtils.isNotBlank(message)) {
            System.err.println(message);
            t.printStackTrace(System.err);
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (PrintStream ps = new PrintStream(baos)) {
            StackTraceElement[] elements = t.getStackTrace();
            if (elements != null && elements.length > 0) {
                ps.print(t.getClass() + " at " + elements[0].getFileName() + ":" + elements[0].getLineNumber());
            }
        } catch (Throwable tt) {
            tt.printStackTrace(System.err);
        }

        String hash = "" + System.nanoTime();
        try {
            byte[] bytes = baos.toByteArray();

            MessageDigest md = MessageDigest.getInstance("SHA1");
            md.update(bytes);
            byte[] digest = md.digest();

            ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
            try (PrintStream ps = new PrintStream(baos2)) {
                for (int i = 0; i < 4; i++) {
                    ps.printf("%02X", digest[i]);
                }
            }

            hash = baos2.toString();
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }

        String fileName = "exception-" + hash + ".txt";

        if (!Files.exists(Path.of(fileName))) {
            try (PrintStream ps = new PrintStream(new FileOutputStream(fileName))) {
                ps.print(new Date() + " | ");
                t.printStackTrace(ps);
            } catch (Throwable e) {
                e.printStackTrace(System.err);
            }
        } else {
            System.err.println("Not capturing exception to a file, already exists: " + fileName);
        }
    }
}