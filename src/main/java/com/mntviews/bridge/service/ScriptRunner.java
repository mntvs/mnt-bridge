package com.mntviews.bridge.service;

import java.io.*;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Tool to run database scripts
 */
public class ScriptRunner {

    private static final String DEFAULT_DELIMITER = ";";
    private static final String SCHEMA_NAME_TEMPLATE = "${schemaName}";
    private static final String VERSION_NAME_TEMPLATE = "${versionStr}";
    private static final Pattern SOURCE_COMMAND = Pattern.compile("^\\s*SOURCE\\s+(.*?)\\s*$", Pattern.CASE_INSENSITIVE);

    /**
     * regex to detect delimiter.
     * ignores spaces, allows delimiter in comment, allows an equals-sign
     */
    public static final Pattern delimP = Pattern.compile("^\\s*(--)?\\s*delimiter\\s*=?\\s*([^\\s]+)+\\s*.*$", Pattern.CASE_INSENSITIVE);

    private final Connection connection;

    private final boolean stopOnError;
    private final boolean autoCommit;

    @SuppressWarnings("UseOfSystemOutOrSystemErr")
    private final PrintWriter logWriter = null;
    @SuppressWarnings("UseOfSystemOutOrSystemErr")
    private PrintWriter errorLogWriter = null;

    private String delimiter = DEFAULT_DELIMITER;
    private boolean fullLineDelimiter = false;

    private final String userDirectory = System.getProperty("user.dir");

    public ScriptRunner(Connection connection, boolean autoCommit,
                        boolean stopOnError) {
        this.connection = connection;
        this.autoCommit = autoCommit;
        this.stopOnError = stopOnError;
        //logWriter = new PrintWriter(System.out);
        errorLogWriter = new PrintWriter(System.out);
    }

    public void setDelimiter(String delimiter, boolean fullLineDelimiter) {
        this.delimiter = delimiter;
        this.fullLineDelimiter = fullLineDelimiter;
    }


    /**
     * Runs an SQL script (read in using the Reader parameter)
     *
     * @param reader - the source of the script
     */
    public void runScript(Reader reader, String schemaName, String versionStr) throws IOException, SQLException {
        try {
            boolean originalAutoCommit = connection.getAutoCommit();
            try {
                if (originalAutoCommit != this.autoCommit) {
                    connection.setAutoCommit(this.autoCommit);
                }
                runScript(connection, reader, schemaName, versionStr);
            } finally {
                connection.setAutoCommit(originalAutoCommit);
            }
        } catch (IOException | SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Error running script.  Cause: " + e, e);
        }
    }

    /**
     * Runs an SQL script (read in using the Reader parameter) using the
     * connection passed in
     *
     * @param conn   - the connection to use for the script
     * @param reader - the source of the script
     * @throws SQLException if any SQL errors occur
     * @throws IOException  if there is an error reading from the Reader
     */
    private void runScript(Connection conn, Reader reader, String schemaName, String versionStr) throws IOException,
            SQLException {
        StringBuffer command = null;
        try {
            LineNumberReader lineReader = new LineNumberReader(reader);
            String line;
            while ((line = lineReader.readLine()) != null) {
                line = line.replace(SCHEMA_NAME_TEMPLATE, schemaName).replace(VERSION_NAME_TEMPLATE, versionStr);
                if (command == null) {
                    command = new StringBuffer();
                }
                String trimmedLine = line.trim();
                final Matcher delimMatch = delimP.matcher(trimmedLine);
                if (trimmedLine.length() < 1
                        || trimmedLine.startsWith("//")) {
                    // Do nothing
                } else if (delimMatch.matches()) {
                    setDelimiter(delimMatch.group(2), false);
                } else if (trimmedLine.startsWith("--")) {
                    println(trimmedLine);
                } else if (!fullLineDelimiter
                        && trimmedLine.endsWith(getDelimiter())
                        || fullLineDelimiter
                        && trimmedLine.equals(getDelimiter())) {
                    command.append(line, 0, line
                            .lastIndexOf(getDelimiter()));
                    command.append(" ");
                    this.execCommand(conn, command, lineReader, schemaName, versionStr);
                    command = null;
                } else {
                    command.append(line);
                    command.append("\n");
                }
            }
            if (command != null) {
                this.execCommand(conn, command, lineReader, schemaName, versionStr);
            }
            if (!autoCommit) {
                conn.commit();
            }
        } catch (IOException e) {
            throw new IOException(String.format("Error executing '%s': %s", command, e.getMessage()), e);
        } finally {
            conn.rollback();
            flush();
        }
    }

    private void execCommand(Connection conn, StringBuffer command,
                             LineNumberReader lineReader, String schemaName,String versionStr) throws IOException, SQLException {

        if (command.length() == 0) {
            return;
        }

        Matcher sourceCommandMatcher = SOURCE_COMMAND.matcher(command);
        if (sourceCommandMatcher.matches()) {
            this.runScriptFile(conn, sourceCommandMatcher.group(1), schemaName, versionStr);
            return;
        }

        this.execSqlCommand(conn, command, lineReader);
    }

    private void runScriptFile(Connection conn, String filepath, String schemaName,String versionStr) throws IOException, SQLException {
        File file = new File(userDirectory, filepath);
        this.runScript(conn, new BufferedReader(new FileReader(file)), schemaName, versionStr);
    }

    private void execSqlCommand(Connection conn, StringBuffer command,
                                LineNumberReader lineReader) throws SQLException {

        Statement statement = conn.createStatement();

        println(command);

        boolean hasResults = false;
        try {
            hasResults = statement.execute(command.toString());
        } catch (SQLException e) {
            final String errText = String.format("Error executing '%s' (line %d): %s",
                    command, lineReader.getLineNumber(), e.getMessage());
            printlnError(errText);
            System.err.println(errText);
            if (stopOnError) {
                throw new SQLException(errText, e);
            }
        }

        if (autoCommit && !conn.getAutoCommit()) {
            conn.commit();
        }

        ResultSet rs = statement.getResultSet();
        if (hasResults && rs != null) {
            ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();
            for (int i = 1; i <= cols; i++) {
                String name = md.getColumnLabel(i);
                print(name + "\t");
            }
            println("");
            while (rs.next()) {
                for (int i = 1; i <= cols; i++) {
                    String value = rs.getString(i);
                    print(value + "\t");
                }
                println("");
            }
        }

        try {
            statement.close();
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    private String getDelimiter() {
        return delimiter;
    }

    @SuppressWarnings("UseOfSystemOutOrSystemErr")

    private void print(Object o) {
        if (logWriter != null) {
            logWriter.print(o);
        }
    }

    private void println(Object o) {
        if (logWriter != null) {
            logWriter.println(o);
        }
    }

    private void printlnError(Object o) {
        if (errorLogWriter != null) {
            errorLogWriter.println(o);
        }
    }

    private void flush() {
        if (logWriter != null) {
            logWriter.flush();
        }
        if (errorLogWriter != null) {
            errorLogWriter.flush();
        }
    }
}