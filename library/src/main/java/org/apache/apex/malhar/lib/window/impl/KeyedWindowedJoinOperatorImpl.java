package org.apache.apex.malhar.lib.window.impl;

import java.util.Map;

import org.apache.apex.malhar.lib.window.JoinAccumulation;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.apex.malhar.lib.window.WindowedStorage;

import com.datatorrent.lib.util.KeyValPair;


/**
 * Keyed Windowed Join Operator
 */
public class KeyedWindowedJoinOperatorImpl<KeyT, InputT1, InputT2, AccumT, OutputT>
    extends AbstractWindowedJoinOperator<KeyValPair<KeyT, InputT1>, KeyValPair<KeyT, InputT2>, KeyValPair<KeyT, OutputT>, WindowedStorage.WindowedKeyedStorage<KeyT, AccumT>, WindowedStorage.WindowedKeyedStorage<KeyT, OutputT>, JoinAccumulation<InputT1, InputT2, ?, ?, ?, AccumT, OutputT>>
{
  // TODO: Add session window support.

  @Override
  public void accumulateTuple2(Tuple.WindowedTuple<KeyValPair<KeyT, InputT2>> tuple)
  {
    KeyValPair<KeyT, InputT2> kvData = tuple.getValue();
    KeyT key = kvData.getKey();
    for (Window window : tuple.getWindows()) {
      // process each window
      AccumT accum = dataStorage.get(window, key);
      if (accum == null) {
        accum = accumulation.defaultAccumulatedValue();
      }
      dataStorage.put(window, key, accumulation.accumulate2(accum, kvData.getValue()));
    }
  }

  @Override
  public void accumulateTuple(Tuple.WindowedTuple<KeyValPair<KeyT, InputT1>> tuple)
  {
    KeyValPair<KeyT, InputT1> kvData = tuple.getValue();
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
  public void fireNormalTrigger(Window window, boolean fireOnlyUpdatedPanes)
  {
    for (Map.Entry<KeyT, AccumT> entry : dataStorage.entrySet(window)) {
      OutputT outputVal = accumulation.getOutput(entry.getValue());
      if (fireOnlyUpdatedPanes) {
        OutputT oldValue = retractionStorage.get(window, entry.getKey());
        if (oldValue != null && oldValue.equals(outputVal)) {
          continue;
        }
      }
      System.out.println(new Tuple.WindowedTuple<>(window, new KeyValPair<>(entry.getKey(), outputVal)));
      output.emit(new Tuple.WindowedTuple<>(window, new KeyValPair<>(entry.getKey(), outputVal)));
      if (retractionStorage != null) {
        retractionStorage.put(window, entry.getKey(), outputVal);
      }
    }
  }

  @Override
  public void fireRetractionTrigger(Window window)
  {
    if (triggerOption.getAccumulationMode() != TriggerOption.AccumulationMode.ACCUMULATING_AND_RETRACTING) {
      throw new UnsupportedOperationException();
    }
    for (Map.Entry<KeyT, OutputT> entry : retractionStorage.entrySet(window)) {
      output.emit(new Tuple.WindowedTuple<>(window, new KeyValPair<>(entry.getKey(), accumulation.getRetraction(entry.getValue()))));
    }
  }
}
