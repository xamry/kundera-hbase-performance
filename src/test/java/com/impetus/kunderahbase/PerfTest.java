package com.impetus.kunderahbase;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kunderahbase.executor.HBaseRunner;

/**
 * 
 */
public class PerfTest
{
    private static Log log = LogFactory.getLog(PerfTest.class);

    @Before
    public void startUp() throws Exception
    {

    }

    @Test
    public void testWrite() throws IOException, InterruptedException
    {
        log.info("running write test");
        HBaseRunner.main(null);
    }

    @Test
    public void testReadByKey() throws IOException, InterruptedException
    {
        log.info("running read by key test");
        HBaseRunner.main(new String[] { "rk" });
    }

    @Test
    public void testReadByColumn() throws IOException, InterruptedException
    {
        log.info("running read by column test");
        HBaseRunner.main(new String[] { "rc", "user_nameCnt" });
    }

    @After
    public void tearDown() throws Exception
    {

    }
}
