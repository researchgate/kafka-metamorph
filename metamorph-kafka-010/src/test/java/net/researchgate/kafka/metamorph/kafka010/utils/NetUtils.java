package net.researchgate.kafka.metamorph.kafka010.utils;

import java.io.IOException;
import java.net.ServerSocket;

class NetUtils {
    private NetUtils() {}

    public static int getRandomPort() {
        try(ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException("Can't get random port");
        }
    }
}
