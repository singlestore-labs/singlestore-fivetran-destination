package com.singlestore.fivetran.destination;

public class Logger {
    private static String escape(String s) {
        return "\"" + s.replace("\"", "\\\"") + "\"";
    }

    private static void log(String level, String message) {
        System.out.println(
            String.format("{" + 
            "\"level\":\"%s\"," + 
            "\"message\": \"%s\"" +
            "\"message-origin\": \"sdk_connector\"" +
            "}", escape(level), escape(message)));
    }

    public static void info(String message) {
        log("INFO", message);
    }

    public static void warning(String message) {
        log("WARNING", message);        
    }

    public static void warning(Exception e) {
        warning(e.toString());        
    }

    public static void warning(String message, Exception e) {
        warning(String.format("%s:\n%s", message, e.toString()));        
    }

    public static void severe(Exception e) {
        log("SEVERE", e.toString());                
    }

    public static void severe(String message) {
        severe(message);                
    }

    public static void severe(String message, Exception e) {
        severe(String.format("%s:\n%s", message, e.toString()));        
    }
}
