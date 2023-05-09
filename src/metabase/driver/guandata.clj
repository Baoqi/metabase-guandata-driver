(ns metabase.driver.guandata
  (:require
    [clojure.java.jdbc :as jdbc]
    [clojure.string :as str]
    [honey.sql :as sql]
    [honey.sql.helpers :as sql.helpers]
    [medley.core :as m]
    [metabase.config :as config]
    [metabase.driver :as driver]
    [metabase.driver.hive-like :as hive-like]
    [metabase.driver.sql-jdbc.common :as sql-jdbc.common]
    [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
    [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
    [metabase.driver.sql-jdbc.sync :as sql-jdbc.sync]
    [metabase.driver.sql.parameters.substitution
     :as sql.params.substitution]
    [metabase.driver.sql.query-processor :as sql.qp]
    [metabase.driver.sql.util :as sql.u]
    [metabase.driver.sql.util.unprepare :as unprepare]
    [metabase.mbql.util :as mbql.u]
    [metabase.query-processor.store :as qp.store]
    [metabase.query-processor.util :as qp.util]
    [metabase.query-processor.util.add-alias-info :as add]
    [metabase.util.honey-sql-2 :as h2x])
  (:import
    (java.sql Connection ResultSet)))

(set! *warn-on-reflection* true)

(driver/register! :guandata, :parent :hive-like)

;;; ------------------------------------------ Custom HoneySQL Clause Impls ------------------------------------------

(def ^:private source-table-alias
  "Default alias for all source tables. (Not for source queries; those still use the default SQL QP alias of `source`.)"
  "t1")

(defmethod sql.qp/->honeysql [:guandata :field]
  [driver [_ _ {::sql.params.substitution/keys [compiling-field-filter?]} :as field-clause]]
  ;; use [[source-table-alias]] instead of the usual `schema.table` to qualify fields e.g. `t1.field` instead of the
  ;; normal `schema.table.field`
  (let [parent-method (get-method sql.qp/->honeysql [:hive-like :field])
        field-clause  (mbql.u/update-field-options field-clause
                                                   update
                                                   ::add/source-table
                                                   (fn [source-table]
                                                     (cond
                                                       ;; DO NOT qualify fields from field filters with `t1`, that won't
                                                       ;; work unless the user-written SQL query is doing the same
                                                       ;; thing.
                                                       compiling-field-filter? ::add/none
                                                       ;; for all other fields from the source table qualify them with
                                                       ;; `t1`
                                                       (integer? source-table) source-table-alias
                                                       ;; no changes for anyone else.
                                                       :else                   source-table)))]
    (parent-method driver field-clause)))

(defn- format-over
  "e.g. ROW_NUMBER() OVER (ORDER BY field DESC) AS __rownum__"
  [_fn [expr partition]]
  (let [[expr-sql & expr-args]           (sql/format-expr expr      {:nested true})
        [partition-sql & partition-args] (sql/format-expr partition {:nested true})]
    (into [(format "%s OVER %s" expr-sql partition-sql)]
          cat
          [expr-args
           partition-args])))

(sql/register-fn! ::over #'format-over)

(defmethod sql.qp/apply-top-level-clause [:guandata :page]
  [_driver _clause honeysql-form {{:keys [items page]} :page}]
  (let [offset (* (dec page) items)]
    (if (zero? offset)
      ;; if there's no offset we can simply use limit
      (sql.helpers/limit honeysql-form items)
      ;; if we need to do an offset we have to do nesting to generate a row number and where on that
      (let [over-clause [::over :%row_number (select-keys honeysql-form [:order-by])]]
        (-> (apply sql.helpers/select (map last (:select honeysql-form)))
            (sql.helpers/from (sql.helpers/select honeysql-form [over-clause :__rownum__]))
            (sql.helpers/where [:> :__rownum__ [:inline offset]])
            (sql.helpers/limit [:inline items]))))))

(defmethod sql.qp/apply-top-level-clause [:guandata :source-table]
  [driver _ honeysql-form {source-table-id :source-table}]
  (let [{table-name :name, schema :schema} (qp.store/table source-table-id)]
    (sql.helpers/from honeysql-form [(sql.qp/->honeysql driver (h2x/identifier :table schema table-name))
                                     [(sql.qp/->honeysql driver (h2x/identifier :table-alias source-table-alias))]])))


;;; ------------------------------------------- Other Driver Method Impls --------------------------------------------

(defmethod sql-jdbc.conn/connection-details->spec :guandata
  [_ {:keys [user password schema host port ssl]
      :or {user "dbuser", password "dbpassword", schema "", host "localhost", port 8080}
      :as details}]
  (-> {:applicationName    config/mb-app-id-string
       :type :guandata
       :subprotocol "guandata"
       :subname (str "//" host ":" port)
       :user user
       :password password
       :host host
       :port port
       :classname "com.guandata.sql.GuandataDriver"
       :loginTimeout 30
       :ssl (boolean ssl)
       :compress 0
       :sendTimeAsDatetime false}
      (sql-jdbc.common/handle-additional-options details)))

(def ^:private database-type->clickhouse-base-type
  (sql-jdbc.sync/pattern-based-database-type->base-type
    [[#"Array" :type/Array]
     [#"Bool" :type/Boolean]
     [#"DateTime64" :type/DateTime]
     [#"DateTime" :type/DateTime]
     [#"Date" :type/Date]
     [#"Decimal" :type/Decimal]
     [#"Enum8" :type/Text]
     [#"Enum16" :type/Text]
     [#"FixedString" :type/TextLike]
     [#"Float32" :type/Float]
     [#"Float64" :type/Float]
     [#"Int8" :type/Integer]
     [#"Int16" :type/Integer]
     [#"Int32" :type/Integer]
     [#"Int64" :type/BigInteger]
     [#"IPv4" :type/IPAddress]
     [#"IPv6" :type/IPAddress]
     [#"Map" :type/Dictionary]
     [#"String" :type/Text]
     [#"Tuple" :type/*]
     [#"UInt8" :type/Integer]
     [#"UInt16" :type/Integer]
     [#"UInt32" :type/Integer]
     [#"UInt64" :type/BigInteger]
     [#"UUID" :type/UUID]]))

(defmethod sql-jdbc.sync/database-type->base-type :guandata
  [_ database-type]
  (let [clickhouse-base-type (database-type->clickhouse-base-type
                               (let [normalized ;; extract the type from Nullable or LowCardinality first
                                     (str/replace (name database-type)
                                                  #"(?:Nullable|LowCardinality)\((\S+)\)"
                                                  "$1")]
                                 (cond
                                   (str/starts-with? normalized "Array(") "Array"
                                   (str/starts-with? normalized "Map(") "Map"
                                   :else normalized)))
        ]

    (if (= clickhouse-base-type :type/*)
      (condp re-matches (name database-type)
        #"boolean"          :type/Boolean
        #"tinyint"          :type/Integer
        #"smallint"         :type/Integer
        #"int"              :type/Integer
        #"bigint"           :type/BigInteger
        #"float"            :type/Float
        #"double"           :type/Float
        #"double precision" :type/Double
        #"decimal.*"        :type/Decimal
        #"char.*"           :type/Text
        #"varchar.*"        :type/Text
        #"string.*"         :type/Text
        #"binary*"          :type/*
        #"date"             :type/Date
        #"time"             :type/Time
        #"timestamp"        :type/DateTime
        #"interval"         :type/*
        #"array.*"          :type/Array
        #"map"              :type/Dictionary
        #".*"               :type/*)
      clickhouse-base-type)
    )
  )

(defn- dash-to-underscore [s]
  (when s
    (str/replace s #"-" "_")))

;; workaround for SPARK-9686 Spark Thrift server doesn't return correct JDBC metadata
(defmethod driver/describe-database :guandata
  [_ database]
  {:tables
   (with-open [conn (jdbc/get-connection (sql-jdbc.conn/db->pooled-connection-spec database))]
     (set
       (for [{:keys [name], table-namespace :namespace} (jdbc/query {:connection conn} ["show tables"])]
         {:name   name ; column name differs depending on server (SparkSQL, hive, Impala)
          :schema "default"})))})

;; workaround for SPARK-9686 Spark Thrift server doesn't return correct JDBC metadata
(defmethod driver/describe-table :guandata
  [driver database {table-name :name, schema :schema}]
  {:name   table-name
   :schema schema
   :fields
   (with-open [conn (jdbc/get-connection (sql-jdbc.conn/db->pooled-connection-spec database))]
     (let [results (jdbc/query {:connection conn} [(format
                                                     "describe %s"
                                                     (sql.u/quote-name driver :table
                                                                       (dash-to-underscore schema)
                                                                       (dash-to-underscore table-name)))])]
       (set
         (for [[idx {col-name :name, data-type :type, :as result}] (m/indexed results)
               ]
           {:name              col-name
            :database-type     data-type
            :base-type         (sql-jdbc.sync/database-type->base-type :guandata (keyword data-type))
            :database-position idx}))))})

;; bound variables are not supported in Spark SQL (maybe not Hive either, haven't checked)
(defmethod driver/execute-reducible-query :guandata
  [driver {{sql :query, :keys [params], :as inner-query} :native, :as outer-query} context respond]
  (let [inner-query (-> (assoc inner-query
                          :remark (qp.util/query->remark :guandata outer-query)
                          :query  (if (seq params)
                                    (binding [hive-like/*param-splice-style* :paranoid]
                                      (unprepare/unprepare driver (cons sql params)))
                                    sql)
                          :max-rows (mbql.u/query->max-rows-limit outer-query))
                        (dissoc :params))
        query       (assoc outer-query :native inner-query)]
    ((get-method driver/execute-reducible-query :sql-jdbc) driver query context respond)))

;; 1.  SparkSQL doesn't support `.supportsTransactionIsolationLevel`
;; 2.  SparkSQL doesn't support session timezones (at least our driver doesn't support it)
;; 3.  SparkSQL doesn't support making connections read-only
;; 4.  SparkSQL doesn't support setting the default result set holdability
(defmethod sql-jdbc.execute/connection-with-timezone :guandata
  [driver database _timezone-id]
  (let [conn (.getConnection (sql-jdbc.execute/datasource-with-diagnostic-info! driver database))]
    (try
      (.setTransactionIsolation conn Connection/TRANSACTION_READ_UNCOMMITTED)
      conn
      (catch Throwable e
        (.close conn)
        (throw e)))))

;; 1.  SparkSQL doesn't support setting holdability type to `CLOSE_CURSORS_AT_COMMIT`
(defmethod sql-jdbc.execute/prepared-statement :guandata
  [driver ^Connection conn ^String sql params]
  (let [stmt (.prepareStatement conn sql
                                ResultSet/TYPE_FORWARD_ONLY
                                ResultSet/CONCUR_READ_ONLY)]
    (try
      (.setFetchDirection stmt ResultSet/FETCH_FORWARD)
      (sql-jdbc.execute/set-parameters! driver stmt params)
      stmt
      (catch Throwable e
        (.close stmt)
        (throw e)))))

;; the current HiveConnection doesn't support .createStatement
(defmethod sql-jdbc.execute/statement-supported? :guandata [_] false)

(doseq [feature [:basic-aggregations
                 :binning
                 :expression-aggregations
                 :expressions
                 :native-parameters
                 :nested-queries
                 :standard-deviation-aggregations]]
  (defmethod driver/supports? [:guandata feature] [_ _] true))

;; only define an implementation for `:foreign-keys` if none exists already. In test extensions we define an alternate
;; implementation, and we don't want to stomp over that if it was loaded already
(when-not (get (methods driver/supports?) [:guandata :foreign-keys])
  (defmethod driver/supports? [:guandata :foreign-keys] [_ _] true))

(defmethod driver/database-supports? [:guandata :test/jvm-timezone-setting]
  [_driver _feature _database]
  false)

(defmethod sql.qp/quote-style :guandata
  [_driver]
  :mysql)
