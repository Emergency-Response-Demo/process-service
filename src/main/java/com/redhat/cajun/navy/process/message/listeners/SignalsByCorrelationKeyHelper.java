package com.redhat.cajun.navy.process.message.listeners;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.dashbuilder.dataset.DataColumn;
import org.dashbuilder.dataset.DataSet;
import org.jbpm.services.api.query.QueryResultMapper;
import org.jbpm.services.api.query.QueryService;
import org.jbpm.services.api.query.model.QueryParam;
import org.kie.internal.query.QueryContext;

public class SignalsByCorrelationKeyHelper {

    public static boolean waitingForSignal(QueryService queryService, String correlationKey, String signal) {
        List<String> signals = queryService.query("signalsByCorrelationKey", new SignalsByCorrelationKeyQueryMapper(), new QueryContext(),
                QueryParam.equalsTo("name", correlationKey));
        return signals.contains(signal);
    }

    public static class SignalsByCorrelationKeyQueryMapper implements QueryResultMapper<List<String>> {

        @Override
        public List<String> map(Object result) {
            if (result instanceof DataSet) {
                DataSet dataSetResult = (DataSet) result;
                List<String> mappedResult = new ArrayList<>();
                if (dataSetResult != null) {
                    for (int i = 0; i < dataSetResult.getRowCount(); i++) {
                        String signal = getColumnStringValue(dataSetResult, "element", i);
                        mappedResult.add(signal);
                    }
                }
                return mappedResult;
            }
            throw new IllegalArgumentException("Unsupported result for mapping " + result);
        }

        private String getColumnStringValue(DataSet currentDataSet, String columnId, int index){
            DataColumn column = currentDataSet.getColumnById( columnId );
            if (column == null) {
                return null;
            }

            Object value = column.getValues().get(index);
            return value != null ? value.toString() : null;
        }

        @Override
        public String getName() {
            return "signals";
        }

        @Override
        public Class<?> getType() {
            return String.class;
        }

        @Override
        public QueryResultMapper<List<String>> forColumnMapping(Map<String, String> columnMapping) {
            return new SignalsByCorrelationKeyQueryMapper();
        }
    }

}
