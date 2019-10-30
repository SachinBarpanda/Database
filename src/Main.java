import java.sql.*;

public class Main {
    /**
     * Declaring every repetative statement to avoid hardcoding
     */
    public static final String DB_Name = "testJava.db";
    public static final String CONNECTION_NAME =
            "jdbc:sqlite:D:\\Practice and Inventions\\Java-Programming\\dbms\\testJava.db";

    public static final String TABLE_CONTACTS = "contacts";

    public static final String COLUMN_NAME = "name";
    public static final String COLUMN_PHONE = "phone";
    public static final String COLUMN_EMAIL = "email";


    public static void main(String[] args) {
        try {
            Connection conn = DriverManager.getConnection(CONNECTION_NAME);
            Statement statement = conn.createStatement();
            statement.execute("DROP TABLE IF EXISTS " + TABLE_CONTACTS);
            statement.execute("CREATE TABLE IF NOT EXISTS " + TABLE_CONTACTS +
                    "(" + COLUMN_NAME + " text, " +
                    COLUMN_PHONE + " integer, " +
                    COLUMN_EMAIL + " text " +
                    ")");

            insertContacts(statement,"TIM",64666,"tim@kali.com");
            insertContacts(statement,"SAM",664566,"sam@gamii.com");
            insertContacts(statement,"JOE",889778,"joey@taimil.com");

/**
 * Do not rerun the insert command or the update cammand or the delete command
 * */

//            statement.execute("INSERT INTO contacts(name , phone ,email)"+
//                    "VALUES ('Samar',934543548798,'sachinongithub@gmail.com')");
//            statement.execute("INSERT INTO contacts(name , phone ,email)"+
//                    "VALUES ('James',93454545448798,'jamesgithub@gmail.com')");
//            statement.execute("INSERT INTO contacts(name , phone ,email)"+
//                    "VALUES ('Dunno',94646546464,'donnogithub@gmail.com')");

//            statement.execute("UPDATE contacts SET phone=989809867 WHERE name='Dunno'");
//            statement.execute("DELETE FROM contacts WHERE name='Sam'");

/**       *********************************************************************/

/**
 * ResultSet statement is the class which helps to show the result
 * */

            statement.execute("SELECT * FROM " + TABLE_CONTACTS);
            ResultSet result = statement.getResultSet();
            while (result.next()) {
                System.out.println(result.getString(COLUMN_NAME) + " " +
                        result.getInt(COLUMN_PHONE) + " " +
                        result.getString(COLUMN_EMAIL));
            }
            result.close();
            statement.close();
            conn.close();

        } catch (SQLException e) {
            System.out.println("Something is wrong " + e.getMessage());
            e.printStackTrace();
        }
    }


    private static void insertContacts(Statement statement,
                                       String name, int phone, String email) throws SQLException {

        statement.execute("INSERT INTO " + TABLE_CONTACTS
                + "(" + COLUMN_NAME + "," +
                COLUMN_PHONE + "," +
                COLUMN_EMAIL +
                ")" + "VALUES ('"+ name +"', "+phone+",'"+email+"')");
    }

}






















