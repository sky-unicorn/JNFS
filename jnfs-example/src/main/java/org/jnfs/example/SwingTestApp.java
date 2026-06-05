package org.jnfs.example;

import org.jnfs.driver.ConnectionState;
import org.jnfs.driver.ConnectionStatus;
import org.jnfs.driver.JNFSDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.*;
import java.awt.*;
import java.io.File;
import java.io.OutputStream;
import java.io.PrintStream;

/**
 * JNFS Swing Test Client
 * Provides a GUI for testing standard file upload and download operations.
 */
public class SwingTestApp extends JFrame {

    private static final Logger LOG = LoggerFactory.getLogger(SwingTestApp.class);

    private static final Color COLOR_CONNECTED = new Color(34, 139, 34);    // green
    private static final Color COLOR_PARTIAL = new Color(218, 165, 32);    // goldenrod / yellow
    private static final Color COLOR_DISCONNECTED = new Color(200, 0, 0);  // red
    private static final Color COLOR_UNKNOWN = Color.GRAY;

    private JTextArea logArea;
    private JTextField uploadPathField;
    private JTextField downloadPathField;
    private JTextField storageIdField;
    private JTextField hostField;
    private JTextField portField;

    // Connection status UI
    private JLabel statusIndicator;
    private JLabel statusText;
    private JButton connectBtn;
    private JTextArea connectionDetailArea;

    // Persistent driver instance
    private JNFSDriver driver;
    private ConnectionStatus currentConnectionStatus = null;

    public SwingTestApp() {
        setTitle("JNFS Swing Test Client");
        setSize(800, 600);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLocationRelativeTo(null);

        initComponents();
        redirectSystemStreams();
        updateConnectionStatus((ConnectionStatus) null, "Not connected");
    }

    private void initComponents() {
        // Connection Status Bar (top)
        JPanel statusBar = createStatusBar();

        // Main Panel for Upload/Download
        JPanel mainPanel = createMainPanel();
        mainPanel.setBorder(BorderFactory.createTitledBorder("Standard Operations"));

        // Log Area
        logArea = new JTextArea();
        logArea.setEditable(false);
        logArea.setFont(new Font("Monospaced", Font.PLAIN, 12));
        JScrollPane scrollPane = new JScrollPane(logArea);
        scrollPane.setBorder(BorderFactory.createTitledBorder("Logs"));

        // Main Layout: status bar on top, then split pane (operations / logs)
        JPanel topWrapper = new JPanel(new BorderLayout());
        topWrapper.add(statusBar, BorderLayout.NORTH);
        topWrapper.add(mainPanel, BorderLayout.CENTER);

        JSplitPane splitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT, topWrapper, scrollPane);
        splitPane.setDividerLocation(300);

        add(splitPane);
    }

    private JPanel createStatusBar() {
        JPanel bar = new JPanel(new BorderLayout(8, 4));
        bar.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createMatteBorder(0, 0, 1, 0, Color.LIGHT_GRAY),
                BorderFactory.createEmptyBorder(6, 10, 6, 10)
        ));

        // Left: colored dot + status text
        JPanel leftPanel = new JPanel(new FlowLayout(FlowLayout.LEFT, 6, 0));
        statusIndicator = new JLabel(" \u25CF "); // Unicode filled circle
        statusIndicator.setFont(new Font("Dialog", Font.BOLD, 16));
        statusIndicator.setForeground(COLOR_UNKNOWN);

        statusText = new JLabel("Not connected");
        statusText.setFont(new Font("Dialog", Font.BOLD, 13));

        leftPanel.add(statusIndicator);
        leftPanel.add(statusText);

        // Right: detail button
        JButton detailBtn = new JButton("Details...");
        detailBtn.addActionListener(e -> showConnectionDetails());

        bar.add(leftPanel, BorderLayout.WEST);
        bar.add(detailBtn, BorderLayout.EAST);

        return bar;
    }

    private JPanel createMainPanel() {
        JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(8, 8, 8, 8);
        gbc.fill = GridBagConstraints.HORIZONTAL;

        // Host/Port + Connect Button
        gbc.gridx = 0; gbc.gridy = 0;
        panel.add(new JLabel("Host:"), gbc);
        gbc.gridx = 1;
        hostField = new JTextField("localhost", 15);
        panel.add(hostField, gbc);

        gbc.gridx = 2;
        panel.add(new JLabel("Port:"), gbc);
        gbc.gridx = 3;
        portField = new JTextField("5368", 5);
        panel.add(portField, gbc);

        gbc.gridx = 4;
        connectBtn = new JButton("Connect");
        connectBtn.addActionListener(e -> runTask(this::doConnect));
        panel.add(connectBtn, gbc);

        // Upload Section
        gbc.gridx = 0; gbc.gridy = 1; gbc.gridwidth = 1;
        panel.add(new JLabel("Upload File:"), gbc);
        gbc.gridx = 1; gbc.gridwidth = 2;
        uploadPathField = new JTextField(20);
        panel.add(uploadPathField, gbc);
        gbc.gridx = 3; gbc.gridwidth = 1;
        JButton browseUploadBtn = new JButton("Browse...");
        browseUploadBtn.addActionListener(e -> chooseFile(uploadPathField, false));
        panel.add(browseUploadBtn, gbc);

        gbc.gridx = 4;
        JButton uploadBtn = new JButton("Upload");
        uploadBtn.addActionListener(e -> runTask(this::doUpload));
        panel.add(uploadBtn, gbc);

        // Storage ID Section
        gbc.gridx = 0; gbc.gridy = 2;
        panel.add(new JLabel("Storage ID:"), gbc);
        gbc.gridx = 1; gbc.gridwidth = 3;
        storageIdField = new JTextField(20);
        panel.add(storageIdField, gbc);

        // Download Section
        gbc.gridx = 0; gbc.gridy = 3; gbc.gridwidth = 1;
        panel.add(new JLabel("Download Dir:"), gbc);
        gbc.gridx = 1; gbc.gridwidth = 2;
        downloadPathField = new JTextField("D:\\data\\jnfs\\download\\", 20);
        panel.add(downloadPathField, gbc);
        gbc.gridx = 3; gbc.gridwidth = 1;
        JButton browseDownloadBtn = new JButton("Browse...");
        browseDownloadBtn.addActionListener(e -> chooseFile(downloadPathField, true));
        panel.add(browseDownloadBtn, gbc);

        gbc.gridx = 4;
        JButton downloadBtn = new JButton("Download");
        downloadBtn.addActionListener(e -> runTask(this::doDownload));
        panel.add(downloadBtn, gbc);

        return panel;
    }

    // --- Connection Status Management ---

    private void updateConnectionStatus(ConnectionStatus status, String message) {
        this.currentConnectionStatus = status;
        SwingUtilities.invokeLater(() -> {
            if (status == null) {
                statusIndicator.setForeground(COLOR_UNKNOWN);
                statusText.setText(message);
            } else {
                ConnectionState state = status.getState();
                switch (state) {
                    case SUCCESS:
                        statusIndicator.setForeground(COLOR_CONNECTED);
                        statusText.setText("Connected: " + message);
                        break;
                    case PARTIAL_SUCCESS:
                        statusIndicator.setForeground(COLOR_PARTIAL);
                        statusText.setText("Partial: " + message);
                        break;
                    default:
                        // REGISTRY_UNREACHABLE, NO_NAMENODE, TOKEN_INVALID, TIMEOUT
                        statusIndicator.setForeground(COLOR_DISCONNECTED);
                        statusText.setText("Disconnected: " + message);
                        break;
                }
            }
        });
    }

    private void showConnectionDetails() {
        if (connectionDetailArea == null) {
            connectionDetailArea = new JTextArea(10, 40);
            connectionDetailArea.setEditable(false);
            connectionDetailArea.setFont(new Font("Monospaced", Font.PLAIN, 12));
        }

        StringBuilder sb = new StringBuilder();
        sb.append("=== Connection Details ===\n");
        if (currentConnectionStatus == null) {
            sb.append("Status: Not connected\n");
            sb.append("Please click [Connect] to establish connection.\n");
        } else {
            sb.append("State: ").append(currentConnectionStatus.getState().name())
              .append(" - ").append(currentConnectionStatus.getMessage()).append("\n");
            sb.append("Reachable Registries: ").append(currentConnectionStatus.getReachableRegistries()).append("\n");
            sb.append("Unreachable Registries: ").append(currentConnectionStatus.getUnreachableRegistries()).append("\n");
            sb.append("Discovered NameNodes: ").append(currentConnectionStatus.getDiscoveredNameNodes()).append("\n");
            if (driver != null) {
                sb.append("Host: ").append(hostField.getText()).append("\n");
                sb.append("Port: ").append(portField.getText()).append("\n");
            }
        }

        connectionDetailArea.setText(sb.toString());
        JOptionPane.showMessageDialog(this, new JScrollPane(connectionDetailArea),
                "Connection Details", JOptionPane.INFORMATION_MESSAGE);
    }

    // --- Connection Logic ---

    private void doConnect() {
        String host = hostField.getText().trim();
        int port;
        try {
            port = Integer.parseInt(portField.getText().trim());
        } catch (NumberFormatException e) {
            System.err.println("Invalid port number.");
            return;
        }

        // Close previous driver if exists
        if (driver != null) {
            driver.close();
            driver = null;
        }

        System.out.println("=== Connecting to " + host + ":" + port + " ===");
        updateConnectionStatus((ConnectionStatus) null, "Connecting...");

        try {
            driver = new JNFSDriver(host, port);
            ConnectionStatus status = driver.initialize();
            updateConnectionStatus(status, status.getMessage());

            System.out.println("Connection result: " + status.getState().name() + " - " + status.getMessage());
        } catch (Exception e) {
            LOG.error("Connection failed", e);
            updateConnectionStatus(new ConnectionStatus(
                    ConnectionState.REGISTRY_UNREACHABLE, "Connection failed",
                    null, null, null), "Connection failed");
            if (driver != null) {
                driver.close();
                driver = null;
            }
        }
    }

    private boolean ensureConnected() {
        if (driver == null || currentConnectionStatus == null || !currentConnectionStatus.isOk()) {
            System.err.println("Not connected. Please click [Connect] first.");
            return false;
        }
        return true;
    }

    // --- Utility ---

    private void chooseFile(JTextField targetField, boolean directoryOnly) {
        JFileChooser chooser = new JFileChooser();
        if (directoryOnly) {
            chooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
        }
        int result = chooser.showOpenDialog(this);
        if (result == JFileChooser.APPROVE_OPTION) {
            targetField.setText(chooser.getSelectedFile().getAbsolutePath());
        }
    }

    private void runTask(Runnable task) {
        new Thread(() -> {
            try {
                task.run();
            } catch (Exception e) {
                LOG.error("Task failed", e);
                e.printStackTrace();
            }
        }).start();
    }

    // --- Redirect System.out/err to JTextArea ---
    private void redirectSystemStreams() {
        OutputStream out = new OutputStream() {
            @Override
            public void write(int b) {
                appendLog(String.valueOf((char) b));
            }

            @Override
            public void write(byte[] b, int off, int len) {
                appendLog(new String(b, off, len));
            }
        };
        System.setOut(new PrintStream(out, true));
        System.setErr(new PrintStream(out, true));
    }

    private void appendLog(String text) {
        SwingUtilities.invokeLater(() -> {
            logArea.append(text);
            logArea.setCaretPosition(logArea.getDocument().getLength());
        });
    }

    // --- Business Logic ---

    private void doUpload() {
        if (!ensureConnected()) return;

        String filePath = uploadPathField.getText().trim();

        if (filePath.isEmpty()) {
            System.err.println("Please select a file to upload.");
            return;
        }

        File file = new File(filePath);
        if (!file.exists() || !file.isFile()) {
            System.err.println("File not found or invalid: " + filePath);
            return;
        }

        try {
            System.out.println("=== Starting Upload: " + file.getName() + " ===");
            long start = System.currentTimeMillis();
            String storageId = driver.uploadFile(file);
            long end = System.currentTimeMillis();

            System.out.printf("Upload Success! Time: %.2f s%n", (end - start) / 1000.0);
            System.out.println("Storage ID: " + storageId);
            SwingUtilities.invokeLater(() -> storageIdField.setText(storageId));
        } catch (Exception e) {
            LOG.error("Upload failed", e);
        }
    }

    private void doDownload() {
        if (!ensureConnected()) return;

        String storageId = storageIdField.getText().trim();
        String downloadDir = downloadPathField.getText().trim();

        if (storageId.isEmpty()) {
            System.err.println("Please provide a Storage ID.");
            return;
        }

        File dlDir = new File(downloadDir);
        if (!dlDir.exists()) {
            dlDir.mkdirs();
        }

        try {
            System.out.println("=== Starting Download ===");
            long start = System.currentTimeMillis();
            File downloadedFile = driver.downloadFile(storageId, downloadDir);
            long end = System.currentTimeMillis();

            System.out.printf("Download Success! Time: %.2f s%n", (end - start) / 1000.0);
            System.out.println("Saved to: " + downloadedFile.getAbsolutePath());
        } catch (Exception e) {
            LOG.error("Download failed", e);
        }
    }

    @Override
    public void dispose() {
        if (driver != null) {
            driver.close();
        }
        super.dispose();
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            new SwingTestApp().setVisible(true);
        });
    }
}
