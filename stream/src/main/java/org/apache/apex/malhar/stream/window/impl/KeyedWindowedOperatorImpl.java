package org.apache.apex.malhar.stream.window.impl;

import com.datatorrent.lib.util.KeyValPair;
import org.apache.apex.malhar.stream.window.Accumulation;
import org.apache.apex.malhar.stream.window.SessionWindowedStorage;
import org.apache.apex.malhar.stream.window.TriggerOption;
import org.apache.apex.malhar.stream.window.Tuple;
import org.apache.apex.malhar.stream.window.Window;
import org.apache.apex.malhar.stream.window.WindowOption;
import org.apache.apex.malhar.stream.window.WindowState;
import org.apache.apex.malhar.stream.window.WindowedKeyedStorage;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by david on 6/21/16.
 */
public class KeyedWindowedOperatorImpl<KeyT, InputValT, AccumT, OutputValT>
    extends AbstractWindowedOperator<KeyValPair<KeyT, InputValT>, AccumT, KeyValPair<KeyT, OutputValT>, WindowedKeyedStorage<KeyT, AccumT>, Accumulation<InputValT, AccumT, OutputValT>>
{

  @Override
  protected void assignSessionWindows(List<Window> windows, long timestamp, Tuple<KeyValPair<KeyT, InputValT>> inputTuple)
  {
    KeyT key = inputTuple.getValue().getKey();
    WindowOption.SessionWindows sessionWindowOption = (WindowOption.SessionWindows) windowOption;
    SessionWindowedStorage<KeyT, AccumT> sessionStorage = (SessionWindowedStorage<KeyT, AccumT>) dataStorage;
    Collection<Map.Entry<Window.SessionWindow, AccumT>> sessionEntries = sessionStorage.getSessionEntries(key, timestamp, sessionWindowOption.getMinGap().getMillis());
    switch (sessionEntries.size()) {
      case 0: {
        // There are no existing windows within the minimum gap. Create a new session window
        Window.SessionWindow<KeyT> sessionWindow = new Window.SessionWindow<>(key, timestamp, 1);
        windowStateMap.put(sessionWindow, new WindowState());
        windows.add(sessionWindow);
        break;
      }
      case 1: {
        // There is already one existing window within the minimum gap. See whether we need to extend the time of that window
        Map.Entry<Window.SessionWindow, AccumT> sessionWindowEntry = sessionEntries.iterator().next();
        Window.SessionWindow<KeyT> sessionWindow = sessionWindowEntry.getKey();
        if (sessionWindow.getBeginTimestamp() <= timestamp && timestamp < sessionWindow.getBeginTimestamp() + sessionWindow.getDurationMillis()) {
          // The session window already covers the event
          windows.add(sessionWindow);
        } else {
          // The session window does not cover the event but is within the min gap
          if (triggerOption.getAccumulationMode() == TriggerOption.AccumulationMode.ACCUMULATING_AND_RETRACTING) {
            // fire a retraction trigger because the session window will be enlarged
            fireRetractionTrigger(sessionWindow);
          }
          // create a new session window that covers the timestamp
          long newBeginTimestamp = Math.min(sessionWindow.getBeginTimestamp(), timestamp);
          long newEndTimestamp = Math.max(sessionWindow.getBeginTimestamp() + sessionWindow.getDurationMillis(), timestamp + 1);
          Window.SessionWindow<KeyT> newSessionWindow =
              new Window.SessionWindow<>(key, newBeginTimestamp, newEndTimestamp - newBeginTimestamp);
          windowStateMap.remove(sessionWindow);
          sessionStorage.migrateWindow(sessionWindow, newSessionWindow);
          windowStateMap.put(newSessionWindow, new WindowState());
        }
        break;
      }
      case 2: {
        // There are two windows that fall within the minimum gap of the timestamp. We need to merge the two windows
        Map.Entry<Window.SessionWindow, AccumT> sessionWindowEntry1 = sessionEntries.iterator().next();
        Map.Entry<Window.SessionWindow, AccumT> sessionWindowEntry2 = sessionEntries.iterator().next();
        Window.SessionWindow<KeyT> sessionWindow1 = sessionWindowEntry1.getKey();
        Window.SessionWindow<KeyT> sessionWindow2 = sessionWindowEntry2.getKey();
        AccumT sessionData1 = sessionWindowEntry1.getValue();
        AccumT sessionData2 = sessionWindowEntry1.getValue();
        if (triggerOption.getAccumulationMode() == TriggerOption.AccumulationMode.ACCUMULATING_AND_RETRACTING) {
          // fire a retraction trigger because the two session windows will be merged to a new window
          fireRetractionTrigger(sessionWindow1);
          fireRetractionTrigger(sessionWindow2);
        }
        long newBeginTimestamp = Math.min(sessionWindow1.getBeginTimestamp(), sessionWindow2.getBeginTimestamp());
        long newEndTimestamp = Math.max(sessionWindow1.getBeginTimestamp() + sessionWindow1.getDurationMillis(),
            sessionWindow2.getBeginTimestamp() + sessionWindow2.getDurationMillis());

        Window.SessionWindow<KeyT> newSessionWindow = new Window.SessionWindow<>(key, newBeginTimestamp, newEndTimestamp - newBeginTimestamp);
        AccumT newSessionData = accumulation.merge(sessionData1, sessionData2);
        sessionStorage.remove(sessionWindow1);
        sessionStorage.remove(sessionWindow2);
        sessionStorage.put(newSessionWindow, key, newSessionData);
        break;
      }
      default:
        throw new IllegalStateException("There are more than two sessions matching one timestamp");
    }
  }

  @Override
  public void accumulateTuple(Tuple.WindowedTuple<KeyValPair<KeyT, InputValT>> tuple)
  {
    KeyValPair<KeyT, InputValT> kvData = tuple.getValue();
    KeyT key = kvData.getKey();
    for (Window window : tuple.getWindows()) {
      // process each window
      AccumT accum = dataStorage.get(window, key);
      if (accum == null) {
        accum = accumulation.defaultAccumulatedValue();
      }
      dataStorage.put(window, key, accumulation.accumulate(accum, kvData.getValue()));
    }
  }

  @Override
  public void fireNormalTrigger(Window window)
  {
    for (Map.Entry<KeyT, AccumT> entry : dataStorage.entrySet(window)) {
      output.emit(new Tuple.WindowedTuple<>(window, window.getBeginTimestamp(),
          new KeyValPair<>(entry.getKey(), accumulation.getOutput(entry.getValue()))));
      if (retractionStorage != null) {
        retractionStorage.put(window, entry.getKey(), entry.getValue());
      }
    }
  }

  @Override
  public void fireRetractionTrigger(Window window)
  {
    if (triggerOption.getAccumulationMode() != TriggerOption.AccumulationMode.ACCUMULATING_AND_RETRACTING) {
      throw new UnsupportedOperationException();
    }
    for (Map.Entry<KeyT, AccumT> entry : retractionStorage.entrySet(window)) {
      output.emit(new Tuple.WindowedTuple<>(window, window.getBeginTimestamp(),
          new KeyValPair<>(entry.getKey(), accumulation.getRetraction(entry.getValue()))));
    }
  }

}
