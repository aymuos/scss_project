package constants;

public class Sql {

    public static String GET_PRE_DATA = ""+

"SELECT\n" +

    "sku_nbr AS sku, \n" +

            "array_agg((\n" +

            "SELECT AS STRUCT t.* EXCEPT(LN_SHELF_RETL_CENTERED_R52), \n" +

            " COALESCE(LN SHELF_RETL CENTERED R52, 0) AS LN SHELF RETL_CENTERED_R52, \n" +

            "O AS PRED START_WK_MINUS_13, \n" +

            "NET_SLS_AMT AS STORE_CNT_NET_SLS_ANT,\n" + "STORE COUNT"+

    "STORE COUNT UNT SLS AS STORE_CNT_UNT_SLS, \n" +

            "STORE COUNT UNT_SLS perm_retl AS STORE_CNT_PERM_RETL_SLS, \n" +

            "STORE COUNT UNT_SLS shelf_retl AS STORE_CNT_SHELF_RETL_SLS, \n" +

            "SKU not matched' AS LOWES_FOLLOW_ASSIGNMENT, \n" +

            "CAST(8 AS FLOAT64) AS PRIOR PERM_RETL, \n" +

            "CAST (B AS FLOAT64) AS PRIOR SHELF_RETL, \n" +

            "0 AS VALID FLAG\n" +

            ")) AS row_data\n" +

            "FROM %s.%s.SKU_SPG_PRE t\n" +

            "GROUP BY SKU_NBR\n" +";";

    public static String GET_POST_DATA =

"SELECT\n" +

    "sku_nbr AS sku, \n" +

    "array_agg((\n" +

            "SELECT AS STRUCT t. EXCEPT (LN_SHELF_RETL_CENTERED_R52), \n" +

    "COALESCE(LN_SHELF_RETL_CENTERED R52, 0) AS LN SHELF RETL CENTERED_R52, \n" +

    "AS PRED START_WK_MINUS_13, \n" +

    "STORE COUNT NET_SLS_AMT AS STORE_CNT_NET_SLS_AMT, \n"+

        "STORE COUNT UNT SLS AS STORE_CNT_UNT_SLS, \n" +

        "STORE COUNT UNT_SLS perm retl AS STORE_CNT_PERM_RETL_SLS,\n" +

        "STORE COUNT UNT SLS shelf_retl AS STORE_CNT_SHELF_RETL_SLS, \n" +

        "'SKU not matched' AS LOWES_FOLLOW_ASSIGNMENT, \n" +

        "CAST (B AS FLOAT64) AS PRIOR_PERM_RETL,\n" +

        "CAST (O AS FLOAT64) AS PRIOR_SHELF_RETL, \n" +

        "AS VALID FLAG\n" +

        " )) AS row data\n"+

            " FROM '%s.s.SKU_SPG_PRED_START_WK_PLUS13' t\n" +

            "GROUP BY SKU_NBR\n"+";";}

    public static string GET_FINANCIAL_DATA = "";

