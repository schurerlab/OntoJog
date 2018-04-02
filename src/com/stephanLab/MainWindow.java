package com.stephanLab;

import org.apache.commons.lang3.ObjectUtils;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by jpt55 on 2/16/2018.
 */
public class MainWindow {
    public JPanel panelMain;
    private JPanel headerText;
    private JPanel startPanel;
    private JPanel cardPanel;
    private JPanel generatePanel;
    private JButton generateOntologyButton;
    private JButton runQCButton;
    private JButton addNewTermsButton;
    private JPanel QCPanel;
    private JPanel addPanel;
    private JButton xButton;
    private JTextField configFileField1;
    private JButton configFileFieldSelect;
    private JButton generate;
    private JTextArea outputLog;
    private JPanel logPanel;
    private JTextField outputDirField;
    private JButton outputDirFieldSelect;

    private Thread currentlyRunningThread = null;

    OwlGenerator gen = null;

    public MainWindow() {
        xButton.setVisible(false);
        generateOntologyButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                CardLayout cl = (CardLayout)cardPanel.getLayout();
                cl.show(cardPanel,"generatePanel");
                xButton.setVisible(true);
            }
        });
        xButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                CardLayout cl = (CardLayout)cardPanel.getLayout();
                cl.show(cardPanel,"startPanel");
                xButton.setVisible(false);
                if(currentlyRunningThread!=null && currentlyRunningThread.isAlive()){
                    currentlyRunningThread.interrupt();
                }
            }
        });
        configFileFieldSelect.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                JFileChooser fc = new JFileChooser();
                fc.showOpenDialog(configFileFieldSelect);
                File selectedFile = fc.getSelectedFile();
                configFileField1.setText(selectedFile.toString());
            }
        });
        generate.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                CardLayout cl = (CardLayout)cardPanel.getLayout();
                cl.show(cardPanel,"logPanel");
                outputLog.setText("");
                Map<String,String> argArray = new HashMap<>();
                argArray.put("settings",configFileField1.getText());
                argArray.put("generate","true");
                if(!outputDirField.getText().equals("") && !outputDirField.getText().isEmpty()){
                    argArray.put("out",outputDirField.getText());
                }
                xButton.setVisible(true);
                OwlGenerator gen = new OwlGenerator();
                gen.logWindow = outputLog;
                try {
                    gen.setSettings(argArray);
                    currentlyRunningThread = new Thread(gen);
                    currentlyRunningThread.start();
                }catch( Exception ex){
                    System.out.println(ex);
                }
            }
        });
        outputDirFieldSelect.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                JFileChooser fc = new JFileChooser();
                fc.showOpenDialog(outputDirFieldSelect);
                File selectedFile = fc.getSelectedFile();
                outputDirField.setText(selectedFile.toString());
            }
        });
    }
}
