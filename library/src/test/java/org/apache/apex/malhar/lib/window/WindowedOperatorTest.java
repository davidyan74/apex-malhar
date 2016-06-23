package org.apache.apex.malhar.lib.window;

import javax.validation.ValidationException;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedStorage;
import org.apache.apex.malhar.lib.window.impl.WindowedOperatorImpl;

/**
 * Unit tests for WindowedOperator
 */
public class WindowedOperatorTest
{
  private void checkFailedValidation(WindowedOperatorImpl windowedOperator, String message)
  {
    try {
      windowedOperator.validate();
      Assert.fail("Should fail validation because " + message);
    } catch (ValidationException ex) {
      return;
    }
  }

  @Test
  public void testValidation() throws Exception
  {
    WindowedOperatorImpl<Long, Long, Long> windowedOperator = new WindowedOperatorImpl<>();
    checkFailedValidation(windowedOperator, "nothing is configured");
    windowedOperator.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());
    checkFailedValidation(windowedOperator, "data storage is not set");
    windowedOperator.setDataStorage(new InMemoryWindowedStorage<Long>());
    checkFailedValidation(windowedOperator, "accumulation is not set");
    windowedOperator.setAccumulation(new Accumulation<Long, Long, Long>()
    {
      @Override
      public Long defaultAccumulatedValue()
      {
        return null;
      }

      @Override
      public Long accumulate(Long accumulatedValue, Long input)
      {
        return null;
      }

      @Override
      public Long merge(Long accumulatedValue1, Long accumulatedValue2)
      {
        return null;
      }

      @Override
      public Long getOutput(Long accumulatedValue)
      {
        return null;
      }

      @Override
      public Long getRetraction(Long accumulatedValue)
      {
        return null;
      }
    });
    windowedOperator.validate();
    windowedOperator.setTriggerOption(new TriggerOption().accumulatingAndRetractingFiredPanes());
    checkFailedValidation(windowedOperator, "retracting storage is not set for ACCUMULATING_AND_RETRACTING");
    windowedOperator.setRetractionStorage(new InMemoryWindowedStorage<Long>());
    windowedOperator.validate();
    windowedOperator.setTriggerOption(new TriggerOption().discardingFiredPanes().firingOnlyUpdatedPanes());
    checkFailedValidation(windowedOperator, "DISCARDING is not valid for option firingOnlyUpdatedPanes");
    windowedOperator.setTriggerOption(new TriggerOption().accumulatingFiredPanes().firingOnlyUpdatedPanes());
    windowedOperator.setRetractionStorage(null);
    checkFailedValidation(windowedOperator, "retracting storage is not set for option firingOnlyUpdatedPanes");
  }

  @Test
  public void testWatermark()
  {
  }

  @Test
  public void testAccumulation()
  {
  }

  @Test
  public void testLateness()
  {
  }

  @Test
  public void testTriggers()
  {
  }
}
