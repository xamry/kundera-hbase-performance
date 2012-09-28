/**
 * 
 */
package com.impetus.kunderahbase.executor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.impetus.kunderahbase.dao.user.UserDao;
import com.impetus.kunderahbase.dao.user.UserDaoFactory;
import com.impetus.kunderahbase.dto.UserHBaseDTO;

/**
 * @author impadmin
 * 
 */
public class KunderaPerformanceRunner
{
    UserDao userDao;

    // List<UserHBaseDTO> users = new ArrayList<UserHBaseDTO>();

    public static Map<String, Long> profiler = new java.util.HashMap<String, Long>();

    public static Map<String, Long> readProfiler = new java.util.HashMap<String, Long>();

    private void init(String client)
    {
        if (userDao == null)
        {
            userDao = UserDaoFactory.getUserDao(client);
        }
        userDao.init();
    }

    /**
     * @param args
     */
    public static void main(String[] args)
    {

        KunderaPerformanceRunner runner = new KunderaPerformanceRunner();
        if (args[2].equalsIgnoreCase("c"))
        {
            runner.run(Integer.parseInt(args[0]), args[1], false, Integer.parseInt(args[3]), args);
        }
        else if (args[2].equalsIgnoreCase("cb"))
        {
            runner.bulkLoadOnConcurrentThreads(Integer.parseInt(args[0]), args[1], Integer.parseInt(args[3]), args);

        }
        else if (args[2].equalsIgnoreCase("b"))
        {
            runner.onBulkLoad(Integer.parseInt(args[0]), args[1], args);
        }
        // runner.run(Integer.parseInt(args[0]),args[1]);
        // runner.close();
    }

    public void run(final int noOfRecords, String client, final boolean isBulkLoad, int noOfThreads, String arg[])
    {
        try
        {
            String type = "cb";
            init(client);
            System.out.println("<<<<<<On Max Concurrent users Insert>>>>>>");
            long t1 = System.currentTimeMillis();
            if (!isBulkLoad)
            {
                type = "c";
                onConcurrentLoad(noOfThreads, 1);
            }
            else
            {
                onConcurrentLoad(noOfThreads, noOfRecords);
            }

            // TODO add Task executor to control threads and close data
            long t2 = System.currentTimeMillis();
            System.out.println("Kundera Performance: MaxinsertUsers(" + noOfRecords
                    + "), total number of records/thread(" + noOfThreads + ")>>>\t" + (t2 - t1) + " For: " + client);
            profiler.put(client + ":" + type + ":" + noOfRecords + ":" + noOfThreads, (t2 - t1));

            onConcurrentRead(noOfThreads, noOfRecords, arg, client);
        }
        catch (Exception e)
        {
            // TODO: handle exception
            e.printStackTrace();
        }
    }

    private void onConcurrentLoad(final int num, final int noOfRecs)
    {
        // ExecutorService executor = Executors.newFixedThreadPool(num);
        // List<Future> tasks = new ArrayList<Future>();
        // Future task = null;
        for (int i = 1; i <= num; i++)
        {
            ConThreadExecutor c = new ConThreadExecutor(userDao, noOfRecs, i);
            try
            {
                c.t.join();
            }
            catch (InterruptedException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    private void onConcurrentTest(int num)
    {

        for (int i = 1; i <= num; i++)
        {
            ConThreadExecutor c = new ConThreadExecutor(userDao, 1, i);
            try
            {
                c.t.join();
            }
            catch (InterruptedException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        /*
         * ExecutorService executor = Executors.newFixedThreadPool(num); for(int
         * i =1 ; i<=num ; i++) { executor.execute(new
         * ConThreadExecutor(userDao, 1, i)); }
         * 
         * try { Thread.sleep(500); executor.shutdown();
         * executor.awaitTermination(500, TimeUnit.MILLISECONDS); } catch
         * (InterruptedException e) { // TODO Auto-generated catch block
         * e.printStackTrace(); }
         */
        // ConThreadExecutor t = new ConThreadExecutor(userDao, 1);
        /*
         * for (final UserDTO user : prepareDataSet(num)) { new Thread(new
         * Runnable() {
         * 
         * @Override public void run() { // init(client);
         * 
         * userDao.insertUser(user); // user=null; } }).start(); }
         */
    }

    public void onBulkLoad(int rangeValue, String client, String[] args)
    {
        init(client);
        List<UserHBaseDTO> userSet = prepareDataSet(rangeValue);
        System.out.println("<<<<<<On Max Single Insert>>>>>>");
        long t1 = System.currentTimeMillis();
        userDao.insertUsers(userSet, true);
        long t2 = System.currentTimeMillis();
        System.out.println("Kundera Performance: MaxinsertUsers(" + 1 + "), total number of records(" + rangeValue
                + ")>>>\t" + (t2 - t1) + " For: " + client);

        profiler.put(client + ":" + "b" + ":" + rangeValue + ":" + "1", (t2 - t1));
        // // this.users.clear();
        // // this.users=null;
        if (userSet.isEmpty())
            userSet = prepareDataSet(rangeValue);
        onReadComputation(args, userSet, true, client, "b", 1);
        close();
    }

    public void bulkLoadOnConcurrentThreads(int rangeValue, String client, int noOfRecs, String arg[])
    {
        run(rangeValue, client, true, noOfRecs, arg);
    }

    public void close()
    {
        userDao.cleanup();
    }

    /**
     * On user persist
     * 
     * @param rangeValue
     *            range value.
     */
    private List<UserHBaseDTO> prepareDataSet(final Integer rangeValue)
    {
        List<UserHBaseDTO> userSet = new ArrayList<UserHBaseDTO>(rangeValue);
        for (int i = 0; i < rangeValue; i++)
        {
            UserHBaseDTO user = new UserHBaseDTO();
            user.setUserId(getString("userId_", i));
            user.setUserName("Amry");
            user.setUserNameCounter(getString("Amry_", i));

            user.setPassword(getString("password_", i));
            user.setRelationshipStatus(getString("relation_", i));
            userSet.add(user);

        }
        return userSet;

    }

    private String getString(String fieldName, int key)
    {
        StringBuilder strBuild = new StringBuilder(fieldName);
        strBuild.append(key);
        return strBuild.toString();
    }

    /**
     * Computation on read.
     * 
     * @param args
     *            arguments.
     */
    private void onReadComputation(String args[], List<UserHBaseDTO> users, boolean isBulk, String client, String type,
            int noOfThreads)
    {

        if (args != null && args.length > 4)
        {
            String pattern = args[4];
            onRead(args, users, isBulk, client, type, noOfThreads, pattern);
        }
    }

    private void onRead(String[] args, List<UserHBaseDTO> users, boolean isBulk, String client, String type,
            int noOfThreads, String pattern)
    {
        if (type.equalsIgnoreCase("b"))
        {
            if (pattern != null && pattern.equalsIgnoreCase("rk"))
            {

                // means find by key and find All.(Single vs. Batch Read)
                long t1 = System.currentTimeMillis();
                userDao.findUserById(isBulk, users);
                long t2 = System.currentTimeMillis();
                readProfiler.put(client + ":" + type + ":" + users.size() + ":" + noOfThreads + ":id:s", (t2 - t1));

                t1 = System.currentTimeMillis();

                // batch read.
                userDao.findAll(users.size());

                t2 = System.currentTimeMillis();

                readProfiler.put(client + ":" + type + ":" + users.size() + ":" + noOfThreads + ":id:b", (t2 - t1));
                users.clear();

            }
            else if (pattern != null && pattern.equalsIgnoreCase("rc"))
            {
                if (!(args.length > 5))
                {
                    throw new IllegalArgumentException(
                            " invalid set of arguments!, Please provide column name as parameter");
                }

                String columnName = args[5];

                // means find by secondry index(Single vs. batch read)
                long t1 = System.currentTimeMillis();

                userDao.findUserByUserName(columnName, isBulk, users);
                long t2 = System.currentTimeMillis();

                readProfiler.put(client + ":" + type + ":" + users.size() + ":" + noOfThreads + ":column:s", (t2 - t1));

                t1 = System.currentTimeMillis();
                userDao.findAllByUserName(users.size());
                t2 = System.currentTimeMillis();

                readProfiler.put(client + ":" + type + ":" + users.size() + ":" + noOfThreads + ":column:b", (t2 - t1));
                users.clear();
            }
        }
        else
        {
            onConcurrentRead(users.size(), noOfThreads, args, client);
        }

    }

    private void onConcurrentRead(final int num, final int noOfRecs, String args[], String client)
    {
        // ExecutorService executor = Executors.newFixedThreadPool(num);
        // List<Future> tasks = new ArrayList<Future>();
        // Future task = null;
        for (int i = 1; i <= num; i++)
        {
            ConReadThreadExecutor c = new ConReadThreadExecutor(userDao, noOfRecs, i, args, client, num);
            try
            {
                c.t.join();
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }
    }
}
