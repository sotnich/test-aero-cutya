package tinkoff.dwh.cut;

import com.aerospike.client.*;
import com.aerospike.client.policy.ScanPolicy;
import tinkoff.dwh.cut.data.ColumnValues;
import tinkoff.dwh.cut.data.ColumnsValues;
import tinkoff.dwh.cut.data.KeyValue;
import tinkoff.dwh.cut.data.TableValues;
import tinkoff.dwh.cut.meta.Column;
import tinkoff.dwh.cut.meta.Table;
import tinkoff.dwh.cut.meta.TableRelation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

public class Utils {
    public static TableValues getTableValues(String tableName, String [][] vals) {
        Table table = new Table(tableName);
        for (int i = 0; i < vals[0].length; i++)
            table.addColumn(new Column(tableName, vals[0][i]));

        TableValues ret = new TableValues(table);
        for (int j = 1; j < vals.length; j++ )
            ret.addRow(vals[j]);

        return ret;
    }

    public static ArrayList<String> loadCSV(String csvFile) {
        ArrayList<String> ret = new ArrayList<String>();
        try {
            BufferedReader br = new BufferedReader(new FileReader(csvFile));
            String line = null;
            while ((line = br.readLine()) != null) {
                ret.add(line);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }

    //Class that extends Comparator
    static class ColumnComparator implements Comparator
    {
        int columnToSort;
        ColumnComparator(int columnToSort) {
            this.columnToSort = columnToSort;
        }
        //overriding compare method
        public int compare(Object o1, Object o2) {
            String[] row1 = (String[]) o1;
            String[] row2 = (String[]) o2;
            //compare the columns to sort
            return row1[columnToSort].compareTo(row2[columnToSort]);
        }
    }

    public static ColumnValues getSingleColumnValues(Table table, ArrayList<String> lines) {
        Column prmColumn = new Column(table.getTableName(), lines.get(0));
        lines.remove(0);
        return new ColumnValues(prmColumn, lines);

    }

    // Для каждого ключа формируем набор значений
    public static HashMap<Column, HashMap<String, ColumnsValues>> getColumnValues(Table table, ArrayList<String> lines) {
        HashMap<Column, HashMap<String, ColumnsValues>> ret = new HashMap<Column, HashMap<String, ColumnsValues>>();

        String [] columnNames = lines.get(0).split("\\;",-1);
        Column [] columns  = new Column[columnNames.length];
        for (int i = 0; i < columnNames.length; i++)
            columns[i] = new Column(table.getTableName(), columnNames[i]);

        String[][] aValues = new String[lines.size() - 1][];
        for (int i = 1; i < lines.size(); i++)
            aValues[i - 1] = lines.get(i).split("\\;", -1);

        for (int colNum = 0; colNum < columns.length; colNum++) {
            Arrays.sort(aValues, new ColumnComparator(colNum));
            Column prmColumn = columns[colNum];
            int i = 0;
            HashSet<String>[] values = new HashSet[columns.length - 1];
            Column[] secColumns = new Column[columns.length - 1];
            for (int j = 0; j < columns.length; j++) {
                int c = j < colNum ? j : j - 1;
                if (j != colNum) {
                    secColumns[c] = columns[j];
                }
            }
            if (!ret.containsKey(prmColumn))
                ret.put(prmColumn, new HashMap<String, ColumnsValues>());
            HashMap<String, ColumnsValues> retColumnArray = ret.get(prmColumn);

            while (i < aValues.length) {

                String prmColValue = aValues[i][colNum];
                boolean finish = false;
                for (int k = 0; k < values.length; k++)
                    values[k] = new HashSet<String>();

                while (i < aValues.length && !finish) {

                    for (int j = 0; j < columns.length; j++) {
                        int c = j < colNum ? j : j - 1;
                        if (j != colNum)
                            values[c].add(aValues[i][j]);
                    }

                    if (i + 1 < aValues.length && aValues[i + 1][colNum].equals(prmColValue))
                        i++;
                    else
                        finish = true;
                }

                ColumnsValues colsVals = new ColumnsValues(secColumns, values);
                retColumnArray.put(prmColValue, colsVals);

                i++;
            }
        }

        return ret;
    }

    public static TableValues getTableValues(String tableName, ArrayList<String> fileLines, int offset, int numCnt) {
        TableValues ret = new TableValues(new Table(tableName, fileLines.get(0).split(";")));

        for (int i = offset; i < Math.min(offset + numCnt, fileLines.size()); i++ ) {
            ret.addRow(fileLines.get(i).split("\\;",-1));
        }

        return ret;
    }

    public static TableValues loadTableValuesFromCSV(String tableName, String csvFile) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(csvFile));
            TableValues ret = new TableValues(new Table(tableName, br.readLine().split(";")));

            String line = "";
            while ((line = br.readLine()) != null) {
                ret.addRow(line.split("\\;",-1));
            }
            return ret;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void initLinkTable(CutLinkTable linkTable, String [][] values) {
        Table loadTable = new Table(linkTable.getTable().getTableName(), values[0]);
        for (int i = 1; i < values.length; i++)
            linkTable.addRow(KeyValue.fromArray(loadTable.getColumns(), values[i]));
    }

    public static void loadFromCSV(CutLinkTable linkTable, String csvFile) {
        ArrayList<Column> columns = linkTable.getTable().getColumns();
        String line = "";
        String cvsSplitBy = ";";
        try {
            BufferedReader br = new BufferedReader(new FileReader(csvFile));
            Table loadTable = new Table(linkTable.getTable().getTableName(), br.readLine().split(cvsSplitBy));

            while ((line = br.readLine()) != null) {
                linkTable.addRow(KeyValue.fromArray(loadTable.getColumns(), line.split("\\;",-1)));
            }
        } catch (Exception e) {
            System.out.println(line);
            e.printStackTrace();
        }
    }

    public static HashMap<String, ArrayList<TableRelation>> loadRelationsFromCSV(String csvFile) {
        HashMap<String, ArrayList<TableRelation>> ret = new HashMap<String, ArrayList<TableRelation>>();
        String line = "";
        String cvsSplitBy = ",";
        try {
            BufferedReader br = new BufferedReader(new FileReader(csvFile));
            br.readLine().split(cvsSplitBy);

            while ((line = br.readLine()) != null) {
                String [] fields = line.split(cvsSplitBy);
                String jobName = fields[0];
                if (!ret.containsKey(jobName))
                    ret.put(jobName, new ArrayList<TableRelation>());
                ret.get(jobName).add(new TableRelation(new Column(fields[1], fields[2]), new Column(fields[3], fields[4]), fields[5]));
            }
        } catch (Exception e) {
            System.out.println(line);
            e.printStackTrace();
        }
        return ret;
    }

    public static HashMap<String, ArrayList<TableRelation>> loadRelationsFromArray(String [][] values) {
        HashMap<String, ArrayList<TableRelation>> ret = new HashMap<String, ArrayList<TableRelation>>();
        for (String [] row : values) {
            String jobName = row[0];
            if (!ret.containsKey(jobName))
                ret.put(jobName, new ArrayList<TableRelation>());
            ret.get(jobName).add(new TableRelation(new Column(row[1], row[2]), new Column(row[3], row[4]), row[5]));

        }
        return ret;
    }

    public static void deleteTable(String tableName, final AerospikeClient client, String namespace) {
        final ScanPolicy policy = new ScanPolicy();
        policy.concurrentNodes = true;
        policy.includeBinData = false;
        client.scanAll(policy, namespace, tableName, new ScanCallback() {
            public void scanCallback(Key key, Record record) throws AerospikeException {
                client.delete(null, key);
            }
        });
    }

    private static class ScanCnt implements ScanCallback {
        private int recordCount;

        public int runTest(AerospikeClient client, String namespace, String tableName) throws AerospikeException {
            ScanPolicy policy = new ScanPolicy();
            policy.concurrentNodes = false;
            policy.includeBinData = false;

            client.scanAll(policy, namespace, tableName, this);
            return recordCount;
        }

        public void scanCallback(Key key, Record record) {
            recordCount++;
        }
    }

    private static class ScanProfile implements ScanCallback {
        private int recordCount = 0;
        private HashMap<Long, Long> m_cntCnt = new HashMap<Long, Long>();

        public void runTest(AerospikeClient client, String namespace, String tableName) throws AerospikeException {
            ScanPolicy policy = new ScanPolicy();
            policy.concurrentNodes = false;
            policy.includeBinData = true;

            client.scanAll(policy, namespace, tableName, this);
        }

        @SuppressWarnings("unchecked")
        public void scanCallback(Key key, Record record) {

//            long cntKey = 0L;
//            for (String binName : record.bins.keySet()) {
//                ArrayList<String> vals = (ArrayList<String>) record.getValue(binName);
//                cntKey += vals.size();
//            }
//
//            if (!m_cntCnt.containsKey(cntKey))
//                m_cntCnt.put(cntKey, 0L);
//            m_cntCnt.put(cntKey, m_cntCnt.get(cntKey) + 1);

            recordCount++;
        }

        public void printStat() {
            System.out.println("      record cnt: " + recordCount);
            for (Long key : m_cntCnt.keySet())
                System.out.println(" " + key + " values in key cnt: " + m_cntCnt.get(key));
        }
    }

    public static int getSetSize(String tableName, final AerospikeClient client, String namespace) {
        ScanCnt scan = new ScanCnt();
        return scan.runTest(client, namespace, tableName);
    }

    public static void printSetProfile(String tableName, final AerospikeClient client, String namespace) {
        ScanProfile scan = new ScanProfile();
        scan.runTest(client, namespace, tableName);
        scan.printStat();
    }

    private static long m_beforeTime;
    private static String m_method;

    public static void startStep(String method) {
        System.out.println("[" + method + "] - start");
        m_beforeTime = System.currentTimeMillis();
        m_method = method;
    }

    public static void finishStep() {
        long currentTime = System.currentTimeMillis();
        int seconds = Math.round((currentTime - m_beforeTime)/1000);
        long millis = (currentTime - m_beforeTime) - seconds * 1000;
        System.out.println("[" + m_method + "] - finish in " + seconds + " secs and " + millis + " millisecs");
    }

    public static ColumnsValues getColumnsValues(HashMap<Column, HashMap<String, ColumnsValues>> values) {
        ColumnsValues ret = new ColumnsValues();
        for (Column prmColumn : values.keySet()) {
            Set<String> prmValues = values.get(prmColumn).keySet();
            ret.addColumnValues(prmColumn, prmValues);
            for (ColumnsValues cv : values.get(prmColumn).values())
                ret.add(cv);
        }
        return ret;
    }
}
