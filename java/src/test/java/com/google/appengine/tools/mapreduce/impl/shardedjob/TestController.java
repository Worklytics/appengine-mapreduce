package com.google.appengine.tools.mapreduce.impl.shardedjob;

import com.google.cloud.datastore.DatastoreOptions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;
import static org.junit.Assert.assertFalse;

import java.util.Iterator;

/**
 * A mock controller used for unit tests. It simply sums the inputs to combine the results.
 *
 */
@EqualsAndHashCode
@RequiredArgsConstructor
public class TestController extends ShardedJobController<TestTask> {

  private static final long serialVersionUID = 2L;

  @Getter
  private final DatastoreOptions datastoreOptions;
  @Getter
  private final int expectedResult;

  @Getter
  private boolean completed = false;


  @Override
  public void completed(Iterator<TestTask> results) {
    int sum = 0;
    while (results.hasNext()) {
      sum += results.next().getResult();
    }
    assertEquals(expectedResult, sum);
    assertFalse(completed);
    completed = true;
  }

  @Override
  public void failed(Status status) {
    fail("Should not have been called");
  }

}
