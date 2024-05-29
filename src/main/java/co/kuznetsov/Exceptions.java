package co.kuznetsov;

import org.apache.commons.lang.StringUtils;
import org.checkerframework.checker.units.qual.A;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Exceptions {

    public static void capture(Throwable t) {
        capture(t, null);
    }

    public static void capture(Throwable t, String message) {
        deleteSomeFiles(100);

        if (StringUtils.isNotBlank(message)) {
            System.err.println(message);
            t.printStackTrace(System.err);
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (PrintStream ps = new PrintStream(baos)) {
            t.printStackTrace(ps);
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
                for (int i = 0; i < 8; i++) {
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

    private static void deleteSomeFiles(int retainCount) {
        try {
            List<Path> paths = Files.find(Path.of("."), 1, (p, a) -> p.toFile().getAbsolutePath().contains("exception-")).toList();
            List<Path> toRetain = new ArrayList<>(paths);
            while (toRetain.size() > retainCount) {
                Files.delete(toRetain.getFirst());
                toRetain.remove(0);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
