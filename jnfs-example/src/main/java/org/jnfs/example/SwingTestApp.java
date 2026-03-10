package org.jnfs.example;

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

    private JTextArea logArea;
    private JTextField uploadPathField;
    private JTextField downloadPathField;
    private JTextField storageIdField;
    private JTextField hostField;
    private JTextField portField;

    public SwingTestApp() {
        setTitle("JNFS Swing Test Client");
        setSize(800, 600);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLocationRelativeTo(null);

        initComponents();
        redirectSystemStreams();
    }

    private void initComponents() {
        // Main Panel for Upload/Download
        JPanel mainPanel = createMainPanel();
        mainPanel.setBorder(BorderFactory.createTitledBorder("Standard Operations"));

        // Log Area
        logArea = new JTextArea();
        logArea.setEditable(false);
        logArea.setFont(new Font("Monospaced", Font.PLAIN, 12));
        JScrollPane scrollPane = new JScrollPane(logArea);
        scrollPane.setBorder(BorderFactory.createTitledBorder("Logs"));

        // Main Layout
        JSplitPane splitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT, mainPanel, scrollPane);
        splitPane.setDividerLocation(250);
        
        add(splitPane);
    }

    private JPanel createMainPanel() {
        JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.insets = new Insets(8, 8, 8, 8);
        gbc.fill = GridBagConstraints.HORIZONTAL;

        // Host/Port Configuration
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

        // Upload Section
        gbc.gridx = 0; gbc.gridy = 1;
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
        String host = hostField.getText();
        int port = Integer.parseInt(portField.getText());
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

        JNFSDriver driver = new JNFSDriver(host, port);
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
        } finally {
            driver.close();
        }
    }

    private void doDownload() {
        String host = hostField.getText();
        int port = Integer.parseInt(portField.getText());
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

        JNFSDriver driver = new JNFSDriver(host, port);
        try {
            System.out.println("=== Starting Download ===");
            long start = System.currentTimeMillis();
            File downloadedFile = driver.downloadFile(storageId, downloadDir);
            long end = System.currentTimeMillis();

            System.out.printf("Download Success! Time: %.2f s%n", (end - start) / 1000.0);
            System.out.println("Saved to: " + downloadedFile.getAbsolutePath());
        } catch (Exception e) {
            LOG.error("Download failed", e);
        } finally {
            driver.close();
        }
    }

    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            new SwingTestApp().setVisible(true);
        });
    }
}
