package ru;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import ru.pojos.*;
import ru.pojos.*;
import ru.pojos.Join;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;

public class ExtractorPOJOs {

    public static List<Join> extractJoinsList(Dataset<Row> rule) {
        Dataset<Row> datasetOfJoins = rule.select(
                explode(rule.col("joins")).as("join"));
        Dataset<Row> joins = datasetOfJoins.select(
                datasetOfJoins.col("join").getField("table_left").as("tableLeft"),
                datasetOfJoins.col("join").getField("entity_left").as("entityLeft"),
                datasetOfJoins.col("join").getField("table_right").as("tableRight"),
                datasetOfJoins.col("join").getField("entity_right").as("entityRight"),
                datasetOfJoins.col("join").getField("type").as("type")
        );
        return joins.select(col("tableLeft"), col("entityLeft"),
                col("tableRight"),col("entityRight"),col("type")).as(Encoders.bean(Join.class)).collectAsList();
    }

    public static List<Variables> extractVariablesList(Dataset<Row> rule) {
        Dataset<Row> datasetOfVariables = rule.select(
                explode(rule.col("variables")).as("variables"));
        Dataset<Row> variables = datasetOfVariables.select(
                datasetOfVariables.col("variables").getField("name").as("name"),
                datasetOfVariables.col("variables").getField("table").as("table"),
                datasetOfVariables.col("variables").getField("field").as("field"),
                datasetOfVariables.col("variables").getField("type").as("type")
        );
        return variables.select(col("name"), col("table"),
                col("field"),col("type")).as(Encoders.bean(Variables.class)).collectAsList();
    }
    public static Map<String,Parameter> extractParametersMap(Dataset<Row> rule) {
        Dataset<Row> datasetOfParameters = rule.select(
                explode(rule.col("parameters")).as("parameters"));
        Dataset<Row> parameters = datasetOfParameters.select(
                datasetOfParameters.col("parameters").getField("name").as("name"),
                datasetOfParameters.col("parameters").getField("value").as("value"),
                datasetOfParameters.col("parameters").getField("type").as("type")
        );
        List<Parameter> list = parameters.select(col("name"), col("value"),
                col("type")).as(Encoders.bean(Parameter.class)).collectAsList();
        HashMap<String, Parameter> parameterHashMap = new HashMap<>();
        list.forEach(p -> parameterHashMap.put(p.getName(),p));
        return parameterHashMap;
    }

    public static Map<Long, Criterias> extractCriteriasMap(Dataset<Row> rule) {
        Dataset<Row> datasetOfCriterias = rule.select(
                explode(rule.col("criterias")).as("criterias"));
        Dataset<Row> criterias = datasetOfCriterias.select(
                datasetOfCriterias.col("criterias").getField("id").as("id"),
                datasetOfCriterias.col("criterias").getField("parameter").as("parameter"),
                datasetOfCriterias.col("criterias").getField("operator").as("operator"),
                datasetOfCriterias.col("criterias").getField("value").as("value")
        );
        List<Criterias> list = criterias.select(col("id"), col("parameter"),
                col("operator"), col("value")).as(Encoders.bean(Criterias.class)).collectAsList();
        HashMap<Long, Criterias> mapCriterias = new HashMap<>();
        list.forEach(cr -> mapCriterias.put(cr.getId(),cr));
        return mapCriterias;
    }

    public static List<CalcSequence> extracteCalcSequenceList(Dataset<Row> rule) {
        Dataset<Row> datasetOfTree = rule.select(
                explode(rule.col("tree")).as("tree"));
        Dataset<Row> tree = datasetOfTree.select(
                datasetOfTree.col("tree").getField("id").as("id"),
                datasetOfTree.col("tree").getField("criterias").as("criterias"),
                datasetOfTree.col("tree").getField("operator").as("operator"),
                datasetOfTree.col("tree").getField("nodes").as("nodes"),
                datasetOfTree.col("tree").getField("loss").as("loss")
        );
        return tree.select(col("id"), col("criterias"), col("operator"),
                col("nodes"), col("loss")).as(Encoders.bean(CalcSequence.class)).collectAsList();
    }
}
