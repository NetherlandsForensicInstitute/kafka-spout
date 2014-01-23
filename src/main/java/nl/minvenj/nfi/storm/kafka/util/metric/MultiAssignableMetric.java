package nl.minvenj.nfi.storm.kafka.util.metric;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.metric.api.IMetric;

/**
 * Metric tracking assignments to multiple named values.
 *
 * @author Netherlands Forensic Institute
 */
public class MultiAssignableMetric<T> implements IMetric {
    private final Map<String, T> _values = new HashMap<String, T>();

    /**
     * Sets the value named {@code metric} to the provided value.
     *
     * @param metric The name of the metric to set.
     * @param value  The value to set.
     */
    public void set(final String metric, final T value) {
        _values.put(metric, value);
    }

    @Override
    public Map<String, T> getValueAndReset() {
        // return a copy of _values
        return new HashMap<String, T>(_values);
    }
}
