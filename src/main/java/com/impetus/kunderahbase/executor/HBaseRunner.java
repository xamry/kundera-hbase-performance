/**
 * 
 */
package com.impetus.kunderahbase.executor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;

/**
 * The Class RuntimeProcessExmp.
 * 
 * @author Kuldeep.mishra
 * @version 1.0
 */
public class HBaseRunner
{
    private static Runtime runtime = Runtime.getRuntime();

    private static HBaseAdmin admin;

    private static String b[] = { "1", /*
                                        * "1000", "4000", "40000", "100000",
                                        * "1000000"
                                        */};

    private static String c[] = { "1", "10"/*
                                            * , "100", "1000", "10000", "40000",
                                            * "50000", "100000"
                                            */};

    private static String cb[] = { "10"/* , "100", "1000" */};

    /**
     * The main method.
     * 
     * @param args
     *            the arguments
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, InterruptedException
    {
        int i;

        // startHadoopDemons();
        // startHbaseMaster();

        String clientArr[] = { "kundera", "native" };
        String runType[] = { "b", "c", "cb" };

        for (String type : runType)
        {
            for (String client : clientArr)
            {

                if (type.equalsIgnoreCase("b"))
                {
                    for (i = 0; i < b.length; i++)
                    {
                        try
                        {
                            if (!client.equals("kundera"))
                            {
                                createTable("User");
                            }
                            if (args != null)
                            {
                                KunderaPerformanceRunner.main(new String[] { new String(b[i]), client, type, "1",
                                        args[0], args.length == 2 ? args[1] : null });
                            }
                            else
                            {
                                KunderaPerformanceRunner.main(new String[] { new String(b[i]), client, type, "1" });
                            }
                            dropTable("User");
                        }
                        catch (Exception e)
                        {
                            e.printStackTrace();
                        }
                    }

                }
                else if (type.equalsIgnoreCase("c"))
                {
                    for (i = 0; i < c.length; i++)
                    {

                        try
                        {
                            if (!client.equals("kundera"))
                            {
                                createTable("User");
                            }
                            if (args != null)
                            {
                                KunderaPerformanceRunner.main(new String[] { "1", client, type, new String(c[i]),
                                        args[0], args.length == 2 ? args[1] : null });
                            }
                            else
                            {
                                KunderaPerformanceRunner.main(new String[] { "1", client, type, new String(c[i]) });
                            }
                            dropTable("User");
                        }
                        catch (Exception e)
                        {
                            e.printStackTrace();
                        }
                    }
                }
                else if (type.equalsIgnoreCase("cb"))
                {

                    for (i = 0; i < cb.length; i++)
                    {

                        try
                        {
                            if (!client.equals("kundera"))
                            {
                                createTable("User");
                            }
                            // KunderaPerformanceRunner.main(new String[] {
                            // "1000", client, type, new String(cb[i]) });
                            if (args != null)
                            {
                                KunderaPerformanceRunner.main(new String[] { "1000", client, type, new String(cb[i]),
                                        args[0], args.length == 2 ? args[1] : null });
                            }
                            else
                            {
                                KunderaPerformanceRunner.main(new String[] { "1000", client, type, new String(cb[i]) });
                            }

                            dropTable("User");
                        }
                        catch (Exception e)
                        {
                            e.printStackTrace();
                        }
                    }
                }
                else
                {
                    System.out.println("please give valid options ie b/c/cb");
                }
                //
                // stopHbaseMaster();
                // stopHadoopDemons();
            }
        }
        String fileName = "performance_hbase_write" + new Date() + ".xls";
        // onGenerateDelta(KunderaPerformanceRunner.profiler, fileName);
        if (args != null && args[0] != null)
        {
            fileName = "performance_hbase_read" + args[0] + new Date() + ".xls";
            onGenerateReadDelta(KunderaPerformanceRunner.readProfiler, fileName);
        }
        else
        {
            onGenerateDelta(KunderaPerformanceRunner.profiler, fileName);
        }
    }

    static void dropTable(String tableName) throws IOException, InterruptedException
    {
        if (admin == null)
        {
            initiateClient();
        }
        if (admin.isTableAvailable(tableName))
        {
            if (admin.isTableEnabled(tableName))
            {
                admin.disableTable(tableName);
            }
            admin.deleteTable(tableName);
        }
        TimeUnit.SECONDS.sleep(2);
    }

    static void createTable(String tableName) throws IOException, InterruptedException
    {
        if (admin == null)
        {
            initiateClient();
        }
        if (admin.isTableAvailable(tableName))
        {
            if (admin.isTableEnabled(tableName))
            {
                admin.disableTable(tableName);
            }
            admin.deleteTable(tableName);
        }
        HTableDescriptor hTable = new HTableDescriptor(tableName);
        hTable.addFamily(new HColumnDescriptor("user_name"));
        hTable.addFamily(new HColumnDescriptor("password"));
        hTable.addFamily(new HColumnDescriptor("relation"));
        hTable.addFamily(new HColumnDescriptor("user_nameCnt"));
        admin.createTable(hTable);
        TimeUnit.SECONDS.sleep(2);
    }

    static boolean initiateClient() throws MasterNotRunningException, ZooKeeperConnectionException
    {
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("hbase.master", "localhost" + ":" + 60000);
        hadoopConf.set("hbase.zookeeper.quorum", "localhost");
        @SuppressWarnings("deprecation")
        HBaseConfiguration conf = new HBaseConfiguration(hadoopConf);
        admin = new HBaseAdmin(conf);
        return false;
    }

    /**
     * @throws IOException
     * @throws InterruptedException
     */
    private static void startHadoopDemons() throws IOException, InterruptedException
    {
        runtime.exec("/usr/local/hadoop-1.0.2/bin/hadoop-daemon.sh start namenode");
        TimeUnit.SECONDS.sleep(5);
        runtime.exec("/usr/local/hadoop-1.0.2/bin/hadoop-daemon.sh start datanode");
        TimeUnit.SECONDS.sleep(5);
        runtime.exec("/usr/local/hadoop-1.0.2/bin/hadoop-daemon.sh start secondarynamenode");
        TimeUnit.SECONDS.sleep(5);
        runtime.exec("/usr/local/hadoop-1.0.2/bin/hadoop-daemon.sh start jobtracker");
        TimeUnit.SECONDS.sleep(5);
        runtime.exec("/usr/local/hadoop-1.0.2/bin/hadoop-daemon.sh start tasktracker");
        TimeUnit.SECONDS.sleep(5);
    }

    /**
     * @param runtime
     * @throws IOException
     * @throws InterruptedException
     */
    private static void startHbaseMaster() throws IOException, InterruptedException
    {
        runtime.exec("/usr/local/hbase-0.92.1/bin/start-hbase.sh");
        TimeUnit.SECONDS.sleep(5);
    }

    /**
     * @param runtime
     * @throws IOException
     * @throws InterruptedException
     */
    private static void stopHbaseMaster() throws IOException, InterruptedException
    {
        runtime.exec("/usr/local/hbase-0.92.1/bin/stop-hbase.sh");
        TimeUnit.SECONDS.sleep(5);
    }

    /**
     * @throws IOException
     * @throws InterruptedException
     * 
     */
    private static void stopHadoopDemons() throws IOException, InterruptedException
    {
        runtime.exec("/usr/local/hadoop-1.0.2/bin/hadoop-daemon.sh stop namenode");
        TimeUnit.SECONDS.sleep(5);
        runtime.exec("/usr/local/hadoop-1.0.2/bin/hadoop-daemon.sh stop datanode");
        TimeUnit.SECONDS.sleep(5);
        runtime.exec("/usr/local/hadoop-1.0.2/bin/hadoop-daemon.sh stop secondarynamenode");
        TimeUnit.SECONDS.sleep(5);
        runtime.exec("/usr/local/hadoop-1.0.2/bin/hadoop-daemon.sh stop jobtracker");
        TimeUnit.SECONDS.sleep(5);
        runtime.exec("/usr/local/hadoop-1.0.2/bin/hadoop-daemon.sh stop tasktracker");
        TimeUnit.SECONDS.sleep(5);
    }

    private static void onGenerateDelta(Map<String, Long> profiledData, String fileName) throws FileNotFoundException,
            IOException
    {
        HSSFWorkbook workBook = new HSSFWorkbook();
        workBook = generateDelta(b, profiledData, workBook, "Bulk", "b");
        workBook = generateDelta(c, profiledData, workBook, "Concurrent", "c");
        workBook = generateDelta(cb, profiledData, workBook, "Concurrent-Bulk", "cb");

        FileOutputStream fos = null;
        try
        {
            fos = new FileOutputStream(new File(fileName));
            workBook.write(fos);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        finally
        {
            if (fos != null)
            {
                try
                {
                    fos.flush();
                    fos.close();
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        }

    }

    private static void onGenerateReadDelta(Map<String, Long> profiledData, String fileName)
            throws FileNotFoundException, IOException
    {
        // String fileName = "performance_mongo.xls";
        HSSFWorkbook workBook = new HSSFWorkbook();
        workBook = generateReadDelta(b, profiledData, workBook, "Bulk", "b");
        workBook = generateReadDelta(c, profiledData, workBook, "Concurrent", "c");
        workBook = generateReadDelta(cb, profiledData, workBook, "Concurrent-Bulk", "cb");

        FileOutputStream fos = null;
        try
        {
            fos = new FileOutputStream(new File(fileName));
            workBook.write(fos);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        finally
        {
            if (fos != null)
            {
                try
                {
                    fos.flush();
                    fos.close();
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        }

    }

    private static HSSFWorkbook generateDelta(String[] type, Map<String, Long> profiledData, HSSFWorkbook workBook,
            String sheetName, String keyType) throws FileNotFoundException, IOException
    {
        String clientArr[] = { "kundera", "native" };
        HSSFSheet sheet = initSheet(workBook, sheetName);

        int nextRowCount = 2;
        for (String t : type)
        {
            int intnoOfthreads = 1;
            String noOfRecord = t;
            // set on no of record and threads.

            if (keyType.equals("c"))
            {
                intnoOfthreads = Integer.parseInt(t);
                noOfRecord = 1 + "";
            }
            else if (keyType.equals("cb"))
            {
                intnoOfthreads = Integer.parseInt(t);
                noOfRecord = 1000 + "";
            }

            String keySeperator = ":" + keyType;

            HSSFRow dataRow = sheet.createRow(nextRowCount);
            HSSFCell noOfThreadCell = dataRow.createCell(0);
            noOfThreadCell.setCellValue(intnoOfthreads);
            HSSFCell noOfRecordsCell = dataRow.createCell(1);
            noOfRecordsCell.setCellValue(noOfRecord);

            int clientCnt = 2;

            // iterate for earch client.
            double[] cellValArr = new double[4];
            int cnt = 0;
            for (String client : clientArr)
            {
                String key = client + keySeperator + ":" + noOfRecord + ":" + intnoOfthreads;
                System.out.println(key);
                Long timeTaken = profiledData.get(key);
                System.out.println(timeTaken);
                HSSFCell clientCell = dataRow.createCell(clientCnt);
                clientCell.setCellValue(timeTaken);
                clientCnt++;
                cellValArr[cnt++] = timeTaken;
            }

            // populate delta.
            HSSFCell kundera_native = dataRow.createCell(clientCnt++);
            kundera_native.setCellValue(((cellValArr[0] - cellValArr[1]) / cellValArr[1]) * 100);
            
            nextRowCount++;
        }

        return workBook;

    }

    private static HSSFWorkbook generateReadDelta(String[] type, Map<String, Long> profiledData, HSSFWorkbook workBook,
            String sheetName, String keyType) throws FileNotFoundException, IOException
    {
        String clientArr[] = { "kundera", "native" };
        HSSFSheet sheet = initReadSheet(workBook, sheetName);

        int nextRowCount = 2;
        for (String t : type)
        {
            int intnoOfthreads = 1;
            String noOfRecord = t;
            // set on no of record and threads.

            if (keyType.equals("c"))
            {
                intnoOfthreads = Integer.parseInt(t);
                noOfRecord = 1 + "";
            }
            else if (keyType.equals("cb"))
            {
                intnoOfthreads = Integer.parseInt(t);
                noOfRecord = 1000 + "";
            }

            String keySeperator = ":" + keyType;

            HSSFRow dataRow = sheet.createRow(nextRowCount);
            HSSFCell noOfThreadCell = dataRow.createCell(0);
            noOfThreadCell.setCellValue(intnoOfthreads);
            HSSFCell noOfRecordsCell = dataRow.createCell(1);
            noOfRecordsCell.setCellValue(noOfRecord);

            int clientCnt = 2;

            // iterate for earch client.
            double[] cellValArr = new double[8];
            int cnt = 0;
            for (String client : clientArr)
            {
                // String key = client + keySeperator + ":" + noOfRecord + ":" +
                // intnoOfthreads;
                // System.out.println(key);
                // Long timeTaken = profiledData.get(key);
                // System.out.println(timeTaken);
                // HSSFCell clientCell = dataRow.createCell(clientCnt);
                // clientCell.setCellValue(timeTaken);
                // clientCnt++;
                // cellValArr[cnt++] = timeTaken;
                String key = client + keySeperator + ":" + noOfRecord + ":" + intnoOfthreads + ":column:s";
                // client + ":" + type + ":" + users.size() + ":" + noOfThreads
                // + ":column:b"
                // System.out.println(key);
                Long timeTaken = profiledData.get(key);
                if (timeTaken == null)
                {
                    // means it is for findbyId.
                    key = client + keySeperator + ":" + noOfRecord + ":" + intnoOfthreads + ":id:s";

                    // batch read.
                    timeTaken = profiledData.get(key);
                    // System.out.println(timeTaken);
                    // clientCnt = populateCell(dataRow, clientCnt, cellValArr,
                    // cnt, timeTaken);
                    HSSFCell clientCell = dataRow.createCell(clientCnt);

                    clientCell.setCellValue(timeTaken);
                    clientCnt++;
                    cellValArr[cnt++] = timeTaken;

                    key = client + keySeperator + ":" + noOfRecord + ":" + intnoOfthreads + ":id:b";
                    timeTaken = profiledData.get(key);
                    // clientCnt = populateCell(dataRow, clientCnt, cellValArr,
                    // cnt, timeTaken);
                    /* HSSFCell */clientCell = dataRow.createCell(clientCnt);

                    clientCell.setCellValue(timeTaken);
                    clientCnt++;
                    cellValArr[cnt++] = timeTaken;

                }
                else
                {
                    timeTaken = profiledData.get(key);
                    // clientCnt = populateCell(dataRow, clientCnt, cellValArr,
                    // cnt, timeTaken);
                    HSSFCell clientCell = dataRow.createCell(clientCnt);

                    clientCell.setCellValue(timeTaken);
                    clientCnt++;
                    cellValArr[cnt++] = timeTaken;

                    key = client + keySeperator + ":" + noOfRecord + ":" + intnoOfthreads + ":column:b";
                    timeTaken = profiledData.get(key);

                    clientCell = dataRow.createCell(clientCnt);

                    clientCell.setCellValue(timeTaken);
                    clientCnt++;
                    cellValArr[cnt++] = timeTaken;
                }

            }

            // populate delta.
            HSSFCell kundera_native_single = dataRow.createCell(clientCnt++);
            kundera_native_single.setCellValue(((cellValArr[0] - cellValArr[2]) / cellValArr[2]) * 100);

            HSSFCell kundera_native_batch = dataRow.createCell(clientCnt);
            kundera_native_batch.setCellValue(((cellValArr[1] - cellValArr[3]) / cellValArr[3]) * 100);

            nextRowCount++;
        }

        return workBook;

    }

    private static HSSFSheet initSheet(HSSFWorkbook workbook, String sheetName)
    {
        HSSFSheet sheet = workbook.createSheet(sheetName);

        HSSFRow row0 = sheet.createRow(0);
        HSSFCell cell0 = row0.createCell(0);
        cell0.setCellValue("Performance Analysis:");

        HSSFRow row1 = sheet.createRow(1);
        HSSFCell cell1 = row1.createCell(0);
        cell1.setCellValue("NoOfThreads");
        HSSFCell cell2 = row1.createCell(1);
        cell2.setCellValue("NoOfRecords");
        HSSFCell cell3 = row1.createCell(2);
        cell3.setCellValue("kundera");
        HSSFCell cell4 = row1.createCell(3);

        cell4.setCellValue("native");

        HSSFCell cell7 = row1.createCell(4);
        cell7.setCellValue("kundera-native(%)");

        return sheet;
    }

    private static HSSFSheet initReadSheet(HSSFWorkbook workbook, String sheetName)
    {
        HSSFSheet sheet = workbook.createSheet(sheetName);

        HSSFRow row0 = sheet.createRow(0);
        HSSFCell cell0 = row0.createCell(0);
        cell0.setCellValue("Performance Analysis:");

        HSSFRow row1 = sheet.createRow(1);
        HSSFCell cell1 = row1.createCell(0);
        cell1.setCellValue("NoOfThreads");
        HSSFCell cell2 = row1.createCell(1);
        cell2.setCellValue("NoOfRecords");

        HSSFCell cell3 = row1.createCell(2);
        cell3.setCellValue("kundera-single");

        HSSFCell cell4 = row1.createCell(3);
        cell4.setCellValue("kundera-batch");

        HSSFCell cell5 = row1.createCell(4);
        cell5.setCellValue("native-single");

        HSSFCell cell6 = row1.createCell(5);
        cell6.setCellValue("native-batch");

        HSSFCell cell11 = row1.createCell(10);
        cell11.setCellValue("kundera-native-single(%)");

        HSSFCell cell12 = row1.createCell(11);
        cell12.setCellValue("kundera-native-batch(%)");

        return sheet;
    }

}
