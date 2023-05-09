(ns metabase.driver.guandata
  "Guandata Driver."
  (:require [metabase.config :as config]
            [metabase.driver :as driver]
            [metabase.driver.sql-jdbc.common :as sql-jdbc.common]
            [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
            [metabase.util.i18n :refer [trs]]))

(driver/register! :guandata, :parent #{:sparksql })

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                             metabase.driver impls                                              |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmethod driver/display-name :guandata [_] "Guandata")
     
;;; +----------------------------------------------------------------------------------------------------------------+
;;; ----------------------------------------------------- Impls ------------------------------------------------------
;;; +----------------------------------------------------------------------------------------------------------------+

(defmethod sql-jdbc.conn/connection-details->spec :guandata
  [_ {:keys [user password schema host port ssl]
      :or {user "dbuser", password "dbpassword", schema "", host "localhost", port 8080}
      :as details}]
  (-> {:applicationName    config/mb-app-id-string
       :type :guandata
       :subprotocol "guandata"
       :subname (str host ":" port)
       :user user
       :password password
       :host host
       :port port
       :classname "com.guandata.sql.GuandataDriver"
       :loginTimeout 10
       :ssl (boolean ssl)
       :sendTimeAsDatetime false}
      (sql-jdbc.common/handle-additional-options details, :seperator-style :semicolon)))
