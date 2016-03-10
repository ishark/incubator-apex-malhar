package com.datatorrent.lib.io.block;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class BlockReaderTest
{
  private BlockReader underTest;

  @Before
  public void setup()
  {
    underTest = new BlockReader();
  }

  @Test
  public void testUriCase()
  {
    underTest.setUri("HDFS://localhost:8020/user/john/myfile.txt");
    Assert.assertEquals("hdfs://localhost:8020/user/john/myfile.txt", underTest.getUri());
  }
}
