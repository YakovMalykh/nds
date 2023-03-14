package ru;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import ru.pojos.*;
import ru.pojos.Join;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Logics {
    /**
     * вся логика приложения
     */
    public static void logic(String pathToRuleJSON, String pathToResponse) {

        SparkSession spark = SparkSession
                .builder()
                .appName("mySpark")
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate();

        DataFilling.fillingDbInitialData(spark);

        Dataset<Row> rule = spark.read()
                .option("multiline", "true")
                .json(pathToRuleJSON);

        // создаю листы и мапы полученные из файла rule.json
        List<Join> joinList = ExtractorPOJOs.extractJoinsList(rule);
        List<Variables> variablesList = ExtractorPOJOs.extractVariablesList(rule);
        Map<String, Parameter> mapOfParameters = ExtractorPOJOs.extractParametersMap(rule);
        Map<Long, Criterias> mapOfCriterias = ExtractorPOJOs.extractCriteriasMap(rule);
        List<CalcSequence> tree = ExtractorPOJOs.extracteCalcSequenceList(rule);


        String fields = createsStringOfFieldsForSQLQuery(variablesList);

        String queryString = createsJoinsQueryString(joinList, fields);

        // соединяю таблицы
        spark.sql(queryString).createOrReplaceTempView("joined_data");

        String queryWithAllCriterias = createsQueryToGetTableByAllParametersUsingTempView(mapOfParameters, mapOfCriterias, tree);
        // получаю таблицу со всеми записями удовлетворяющими условиям
        spark.sql(queryWithAllCriterias).createOrReplaceTempView("result");
        // формирую временную таблицу с полями, которые требуется записать в ответ
        Dataset<Row> response = spark.sql("select fid, year, quarter from result");
        response.write().json(pathToResponse+"/response");


    }

    /**
     * генерирует запрос для получения всех записей по указанным в правилах условиям
     */
    private static String createsQueryToGetTableByAllParametersUsingTempView(Map<String, Parameter> mapOfParameters, Map<Long, Criterias> mapOfCriterias, List<CalcSequence> tree) {
        List<String> listOfConditions = createsListOfAllConditions(mapOfParameters, mapOfCriterias, tree);

        StringBuilder queryWithConditions = new StringBuilder("select * from joined_data where ");
        for (int i = 0; i < listOfConditions.size(); i++) {
            queryWithConditions.append(listOfConditions.get(i));
            if (i != listOfConditions.size()-1) {
                queryWithConditions.append(" AND ");
            }
        }
        return queryWithConditions.toString();
    }

    private static List<String> createsListOfAllConditions(Map<String, Parameter> mapOfParameters, Map<Long, Criterias> mapOfCriterias, List<CalcSequence> tree) {
        List<String> listOfConditions = null;
        for (int i = 0; i < tree.size(); i++) {
            List<Long> listOfCriteryIds = tree.get(i).getCriterias();
            if (listOfCriteryIds != null) {
                listOfConditions = getListOfStringsFromCriteriasForCreatingQuery(mapOfParameters, mapOfCriterias, listOfCriteryIds);
            }
        }
        return listOfConditions;
    }

    private static List<String> getListOfStringsFromCriteriasForCreatingQuery(Map<String, Parameter> mapOfParameters, Map<Long, Criterias> mapOfCriterias, List<Long> listOfCriteryIds) {
        return listOfCriteryIds.stream().map(id -> {
            Criterias critery = mapOfCriterias.get(id);
            StringBuilder conditionBuilder = new StringBuilder(" ");

            String parameterName = critery.getParameter();

            conditionBuilder.append(isParameterLikeThisExists(parameterName, mapOfParameters)).append(chooseOperator(critery.getOperator()))
                    .append(trimBrackets(critery.getValue())).append(" ");

            return conditionBuilder.toString();

        }).collect(Collectors.toList());
    }
    private static String createsStringOfFieldsForSQLQuery(List<Variables> variablesList) {
        StringBuilder f = new StringBuilder("");
        variablesList.forEach(v -> {
            f.append(spliterator(v.getTable()))
                    .append(".").append(v.getName()).append(", ");
        });
        return f.replace(f.length() - 2, f.length() - 1, "").toString();
    }

    /**
     * метод генерирует alias для таблицы
     */
    private static String spliterator(String tableName) {
        String[] split = tableName.split("_");
        int lastElementsIndex = split.length - 1;
        return split[lastElementsIndex];
    }

    /**
     * генерирует запрос на соединения по указанным полям
     */
    private static String createsJoinsQueryString(List<Join> joinList, String fields) {

        StringBuilder builder = new StringBuilder("SELECT " + fields + " FROM ");

        Join element = joinList.get(0);
        String leftTableAlias = spliterator(element.getTableLeft());
        String rightTableAlias = spliterator(element.getTableRight());

        builder.append(element.getTableLeft()).append(" as ").append(leftTableAlias).append(" ")
                .append(element.getType()).append(" JOIN ").append(element.getTableRight())
                .append(" as ").append(rightTableAlias).append(" ON ")
                .append(leftTableAlias).append('.').append(element.getEntityLeft())
                .append(" == ").append(rightTableAlias).append('.').append(element.getEntityRight());

        for (int i = 1; i < joinList.size(); i++) {
            Join nextElement = joinList.get(i);
            builder.append(" AND ").append(leftTableAlias).append('.').append(nextElement.getEntityLeft())
                    .append(" == ").append(rightTableAlias).append('.').append(nextElement.getEntityRight());
        }

        return builder.toString();
    }
    private static String isParameterLikeThisExists(String parameterName, Map<String, Parameter> mapOfParameters) {
        if (mapOfParameters.containsKey(parameterName)) {
            Parameter parameter = mapOfParameters.get(parameterName);
            // здесь следует еще раз проверить, может в строке есть еще какой-то параметр расчетный...
            return "(" + parameter.getValue() + ")";
        } else {
            return parameterName;
        }
    }
    private static String chooseOperator(String str) {
        switch (str) {
            case "lt":
                return " < ";
            case "gt":
                return " > ";
            default:
                return " = ";
        }
    }
    private static String trimBrackets(String str) {
        return str.replace("{", "").replace("}", "");
    }
}
