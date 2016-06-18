package org.apache.apex.malhar.stream.window;

import java.util.ArrayList;
import java.util.List;

/**
 * All tuples that use the WindowedOperator must be an implementation of this interface
 */
public interface Tuple<T>
{
  /**
   * Gets the value of the tuple
   *
   * @return
   */
  T getValue();

  class PlainTuple<T> implements Tuple<T>
  {
    private T value;

    public PlainTuple()
    {
    }

    public PlainTuple(T value)
    {
      this.value = value;
    }

    public T getValue()
    {
      return value;
    }

    public void setValue(T value)
    {
      this.value = value;
    }
  }

  class TimestampedTuple<T> extends PlainTuple<T>
  {
    private long timestamp;

    public TimestampedTuple()
    {
    }

    public TimestampedTuple(long timestamp, T value)
    {
      super(value);
      this.timestamp = timestamp;
    }

    public long getTimestamp()
    {
      return timestamp;
    }

    public void setTimestamp(long timestamp)
    {
      this.timestamp = timestamp;
    }
  }

  class WindowedTuple<T> extends TimestampedTuple<T>
  {
    private List<Window> windows = new ArrayList<>();

    public WindowedTuple()
    {
    }

    public WindowedTuple(Window window, long timestamp, T value)
    {
      super(timestamp, value);
      this.windows.add(window);
    }

    public List<Window> getWindows() {
      return windows;
    }

    public void addWindow(Window window) {
      this.windows.add(window);
    }
  }

  class WatermarkTuple<T> extends TimestampedTuple<T> implements Watermark
  {
    public WatermarkTuple(long timestamp)
    {
      super(timestamp, null);
    }
  }

}
