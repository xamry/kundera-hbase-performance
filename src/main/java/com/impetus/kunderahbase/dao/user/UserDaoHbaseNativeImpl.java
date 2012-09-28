package com.impetus.kunderahbase.dao.user;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.impetus.kunderahbase.dto.UserHBaseDTO;

public class UserDaoHbaseNativeImpl implements UserDao
{

    private Configuration config;

    private HTablePool hTablePool;

    @Override
    public void init()
    {
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("hbase.master", "localhost" + ":" + 60000);
        hadoopConf.set("hbase.zookeeper.quorum", "localhost");
        config = new HBaseConfiguration(hadoopConf);
        hTablePool = new HTablePool(config, 100);
    }

    @Override
    public void insertUsers(List<UserHBaseDTO> users, boolean isBulk)
    {

        for (int i = 0; i < users.size(); i++)
        {
            UserHBaseDTO user = users.get(i);

            insertUser(user);
        }
    }

    @Override
    public void insertUser(UserHBaseDTO user)
    {
        HTable hTable;
        try
        {
            hTable = (HTable) hTablePool.getTable("User");
            Put p = new Put(Bytes.toBytes(user.getUserId()));
            p.add(Bytes.toBytes("user_name"), Bytes.toBytes("user_name"), user.getUserName().getBytes());
            p.add(Bytes.toBytes("password"), Bytes.toBytes("password"), user.getPassword().getBytes());
            p.add(Bytes.toBytes("relation"), Bytes.toBytes("relation"), user.getRelationshipStatus().getBytes());
            p.add(Bytes.toBytes("user_nameCnt"), Bytes.toBytes("user_nameCnt"), user.getRelationshipStatus().getBytes());
            hTable.put(p);
            hTablePool.putTable(hTable);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public void updateUser(UserHBaseDTO userDTO)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void deleteUser(String userId)
    {
        // TODO Auto-generated method stub

    }

    @Override
    public void cleanup()
    {
        try
        {
            hTablePool.closeTablePool("User");
        }
        catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kunderahbase.dao.user.UserDao#findUserById(boolean,
     * java.util.List)
     */
    @Override
    public void findUserById(boolean isBulk, List<UserHBaseDTO> users)
    {
        Scan scan = null;
        ResultScanner scanner = null;
        if (isBulk)
        {
            scan = new Scan();
        }
        for (UserHBaseDTO u : users)
        {
            int counter = 0;
            byte[] rowKeyBytes = Bytes.toBytes(u.getUserId());
            Get g = new Get(rowKeyBytes);
            scan = new Scan(g);
            assert scan != null;
            try
            {
                scanner = hTablePool.getTable("User").getScanner(scan);
            }
            catch (IOException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            assert scanner != null;
            for (Result result : scanner)
            {
                counter++;
                assert result != null;
            }
            assert counter != 0;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kunderahbase.dao.user.UserDao#findUserByUserName(java.lang
     * .String, boolean, java.util.List)
     */
    @Override
    public void findUserByUserName(String userName, boolean isBulk, List<UserHBaseDTO> users)
    {
        for (UserHBaseDTO user : users)
        {
            Filter filter = new SingleColumnValueFilter(Bytes.toBytes("user_name"), Bytes.toBytes("user_name"),
                    CompareOp.EQUAL, Bytes.toBytes(user.getUserNameCounter()));
            Scan scan = new Scan();
            scan.setFilter(filter);
            scan.addColumn(Bytes.toBytes("user_name"), Bytes.toBytes("user_name"));
            scan.addColumn(Bytes.toBytes("user_nameCnt"), Bytes.toBytes("user_nameCnt"));
            scan.addColumn(Bytes.toBytes("password"), Bytes.toBytes("password"));
            scan.addColumn(Bytes.toBytes("relation"), Bytes.toBytes("relation"));

            ResultScanner scanner = null;
            int counter = 0;
            try
            {
                scanner = hTablePool.getTable("User").getScanner(scan);
            }
            catch (IOException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            assert scanner != null;
            for (Result result : scanner)
            {
                counter++;
                assert result != null;
            }
            assert counter != 0;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kunderahbase.dao.user.UserDao#findAll(int)
     */
    @Override
    public void findAll(int count)
    {
        Scan scan = new Scan();
        ResultScanner scanner = null;
        int counter = 0;
        try
        {
            scanner = hTablePool.getTable("User").getScanner(scan);
        }
        catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        assert scanner != null;
        for (Result result : scanner)
        {
            counter++;
            assert result != null;
        }
        assert counter != 0;
        assert counter == count;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kunderahbase.dao.user.UserDao#findAllByUserName(int)
     */
    @Override
    public void findAllByUserName(int count)
    {
        Filter filter = new SingleColumnValueFilter(Bytes.toBytes("user_name"), Bytes.toBytes("user_name"),
                CompareOp.EQUAL, Bytes.toBytes("Amry"));
        Scan scan = new Scan();
        scan.setFilter(filter);
        scan.addColumn(Bytes.toBytes("user_name"), Bytes.toBytes("user_name"));
        scan.addColumn(Bytes.toBytes("user_nameCnt"), Bytes.toBytes("user_nameCnt"));
        scan.addColumn(Bytes.toBytes("password"), Bytes.toBytes("password"));
        scan.addColumn(Bytes.toBytes("relation"), Bytes.toBytes("relation"));

        ResultScanner scanner = null;
        int counter = 0;
        try
        {
            scanner = hTablePool.getTable("User").getScanner(scan);
        }
        catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        assert scanner != null;
        for (Result result : scanner)
        {
            counter++;
            assert result != null;
        }
        assert counter != 0;
        assert counter == count;
    }
}
