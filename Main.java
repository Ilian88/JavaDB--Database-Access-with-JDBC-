import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class Main {


    public static final String CONNECTION_STRING = "jdbc:mysql://localhost:3306/";
    public static final String DATABASE_NAME = "minions_db";
    public static final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    public static Connection connection;

    public static void main(String[] args) throws SQLException, IOException {

        connection = getConnection();

        System.out.println("Enter exercise number :");
        int exNumber = Integer.parseInt(reader.readLine());

        switch (exNumber) {
            case 2 -> exTwo();
            case 3 -> exThree();
            case 4 -> exFour();
            case 5 -> exFive();
            case 6 -> exSix();
            case 7 -> exSeven();
            case 8 -> exEight();
            case 9 -> exNine();
        }

    }

    private static void exEight() throws IOException, SQLException {
        System.out.println("Enter ids: ");
        int[] ids = Arrays.stream(reader.readLine().split("\\s+"))
                .mapToInt(Integer::parseInt).toArray();
        for (int minionId : ids) {
            changeAgeAndNameOfMinionById(minionId);
        }

        Map<String, String> nameAndAge = getMinionNameAndAge();

        for (Map.Entry<String,String> entry : nameAndAge.entrySet()){
            System.out.printf("%s %s%n",entry.getKey(),entry.getValue());
    }

    }

    private static void changeAgeAndNameOfMinionById(int minionId) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement("UPDATE minions\n" +
                "SET age = age +1 , name = LOWER(name)\n" +
                "WHERE id = ?;");
        preparedStatement.setInt(1, minionId);
        preparedStatement.executeUpdate();

    }

    private static Map<String, String> getMinionNameAndAge() throws SQLException {
        Map<String, String> result = new LinkedHashMap<>();
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM minions ORDER BY id;");

        ResultSet rs = preparedStatement.executeQuery();

        while (rs.next()) {
            result.put(rs.getString("name"), rs.getString("age"));
        }
        return result;

    }

    private static void exFour() throws SQLException, IOException {
        Set<String> listTowns = getAllTownNames();
        Set<String> villainNames = getAllVillains();
        System.out.println("Enter minion info:");
        String[] minionInfo = reader.readLine().split(": ");
        System.out.println("Enter villain name ");
        String villainName = reader.readLine().split(": ")[1];
        String minionName = minionInfo[1].split("\\s+")[0];
        String townName = minionInfo[1].split("\\s+")[2];

        if (!listTowns.contains(townName)) {
            insertTownInDatabase(townName);
            System.out.printf("Town %s was added to the database.%n", townName);
        }
        if (!villainNames.contains(villainName)) {
            insertVillainNameInDatabase(villainName);
            System.out.printf("Villain %s was added to the database.%n", villainName);
        }

        connectMinionToVillain(villainName, minionName);

    }

    private static void connectMinionToVillain(String villainName, String minionName) throws SQLException {
        PreparedStatement preparedStatement =
                connection.prepareStatement("UPDATE minions_villains AS mv\n" +
                        "JOIN villains v on mv.villain_id = v.id\n" +
                        "JOIN minions m on mv.minion_id = m.id\n" +
                        "SET mv.minion_id= m.id\n" +
                        "WHERE v.name = ?;");
        preparedStatement.setString(1, villainName);

        System.out.printf("Successfully added %s to be minion of %s.", minionName, villainName);
    }

    private static void insertVillainNameInDatabase(String villainName) throws SQLException {
        PreparedStatement preparedStatement =
                connection.prepareStatement("INSERT INTO villains (`name`,'evilness_factor')\n" +
                        "VALUES(?,'evil');");
        preparedStatement.setString(1, villainName);

    }

    private static void insertTownInDatabase(String townName) throws SQLException {
        PreparedStatement preparedStatement =
                connection.prepareStatement("INSERT INTO towns (`name`)\n" +
                        "VALUES(?);");
        preparedStatement.setString(1, townName);

    }

    private static Set<String> getAllVillains() throws SQLException {
        Set<String> result = new HashSet<>();
        PreparedStatement preparedStatement =
                connection.prepareStatement("SELECT `name` FROM villains;");
        ResultSet resultSet = preparedStatement.executeQuery();

        while (resultSet.next()) {
            result.add(resultSet.getString(1));
        }
        return result;
    }

    private static Set<String> getAllTownNames() throws SQLException {
        Set<String> result = new HashSet<>();
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT name FROM towns");
        ResultSet rs = preparedStatement.executeQuery();

        while (rs.next()) {
            result.add(rs.getString(1));
        }
        return result;
    }

    private static void exSix() throws IOException, SQLException {
        System.out.println("Enter villain id:");
        int villain_id = Integer.parseInt(reader.readLine());

        int affectedEntities = deleteMinionsByVillainId(villain_id);

        String villainName = getVillianNameById(villain_id);
        deleteVillianById(villain_id);

        System.out.printf("%s was deleted\n" +
                "%d minions released%n", villainName, affectedEntities);
    }

    private static void deleteVillianById(int villain_id) throws SQLException {
        PreparedStatement preparedStatement =
                connection.prepareStatement("DELETE FROM villains WHERE id=?;");

        preparedStatement.setInt(1, villain_id);

        preparedStatement.executeUpdate();
    }

    private static String getVillianNameById(int villain_id) throws SQLException {
        PreparedStatement preparedStatement = connection
                .prepareStatement("SELECT `name` FROM villains  AS v\n" +
                        "WHERE v.id = ?;");
        preparedStatement.setInt(1, villain_id);

        ResultSet rs = preparedStatement.executeQuery();

        if (rs.next() == false) {
            return "No such villain was found";
        }
        ;
        return rs.getString("name");
    }

    private static int deleteMinionsByVillainId(int villain_id) throws SQLException {
        PreparedStatement preparedStatement = connection
                .prepareStatement("DELETE FROM minions_villains WHERE villain_id = ?;");
        preparedStatement.setInt(1, villain_id);


        return preparedStatement.executeUpdate();
    }


    private static void exNine() throws IOException, SQLException {
        System.out.println("Enter minion id :");
        int minion_id = Integer.parseInt(reader.readLine());

        CallableStatement callableStatement = connection
                .prepareCall("CALL usp_get_older(?)");
        callableStatement.setInt(1, minion_id);

        int affected = callableStatement.executeUpdate();
        System.out.println(affected);
    }

    private static void exSeven() throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT name FROM minions");
        ResultSet rs = preparedStatement.executeQuery();
        List<String> allMinionNames = new ArrayList<>();

        while (rs.next()) {
            allMinionNames.add(rs.getString(1));
        }

        int start = 0;
        int end = allMinionNames.size() - 1;

        for (int i = 0; i < allMinionNames.size(); i++) {
            System.out.println(i % 2 == 0
                    ? allMinionNames.get(start++)
                    : allMinionNames.get(end--));
        }
    }

    private static void exFive() throws IOException, SQLException {
        System.out.println("Enter country name :");
        String countryName = reader.readLine();

        PreparedStatement preparedStatement =
                connection.prepareStatement("UPDATE towns SET name = UPPER(name) WHERE  country = ?;");
        preparedStatement.setString(1, countryName);

        int affectedRows = preparedStatement.executeUpdate();
        if (affectedRows == 0) {
            System.out.println("No town names were affected.");
            return;
        }

        System.out.println(String.format("%d town names were affected. ", affectedRows));

        PreparedStatement preparedStatementTowns =
                connection.prepareStatement("SELECT name FROM towns WHERE country = ?;");

        preparedStatementTowns.setString(1, countryName);

        ResultSet rs = preparedStatementTowns.executeQuery();

        while (rs.next()) {
            System.out.println(rs.getString("name"));
        }

    }

    private static void exThree() throws IOException, SQLException {
        System.out.println("Enter villain id:");
        int villainId = Integer.parseInt(reader.readLine());

        String villainName = findVillainNameById(villainId);
        System.out.println(villainName);
        Set<String> output = getAllMinionsById(villainId);
        for (String s : output) {
            System.out.println(" " + s);
        }


    }

    private static Set<String> getAllMinionsById(int villainId) throws SQLException {
        Set<String> result = new LinkedHashSet<>();
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT m.name , m.age\n" +
                " FROM minions AS m\n" +
                "JOIN minions_villains mv on m.id = mv.minion_id\n" +
                "WHERE mv.villain_id = ?;");
        preparedStatement.setInt(1, villainId);

        ResultSet resultSet = preparedStatement.executeQuery();


        int counter = 0;
        while (resultSet.next()) {
            result.add(String.format("%d. %s %d",
                    ++counter,
                    resultSet.getString("name"),
                    resultSet.getInt("age")));
        }

        return result;
    }

    private static String findVillainNameById(int villainId) throws SQLException {
        PreparedStatement preparedStatement =
                connection.prepareStatement("SELECT name FROM villains WHERE id = ?;");
        preparedStatement.setInt(1, villainId);
        StringBuilder sb = new StringBuilder();
        ResultSet resultSet = preparedStatement.executeQuery();

        if (!resultSet.isBeforeFirst()) {
            return String.format("No villain with ID %d exists in the database.", villainId);

        }

        resultSet.next();
        return sb.append("Villain: ")
                .append(resultSet.getString("name")).toString();

    }

    private static void exTwo() throws SQLException {
        PreparedStatement preparedStatement =
                connection.prepareStatement("SELECT v.name , COUNT(DISTINCT mv.minion_id) AS \"count\"\n" +
                        "FROM villains AS v\n" +
                        "JOIN minions_villains mv on v.id = mv.villain_id\n" +
                        "GROUP BY v.name\n" +
                        "HAVING count > ? ;");

        preparedStatement.setInt(1, 15);

        ResultSet resultSet = preparedStatement.executeQuery();

        while (resultSet.next()) {
            System.out.printf("%s %d%n", resultSet.getString(1)
                    , resultSet.getInt(2));
        }
    }

    private static Connection getConnection() throws IOException, SQLException {
//        System.out.println("Enter user:");
//        String user = reader.readLine();
//        System.out.println("Enter user:");
//        String password = reader.readLine();

        Properties props = new Properties();
        props.setProperty("user", "root");
        props.setProperty("password", "12345");

        return DriverManager
                .getConnection(CONNECTION_STRING + DATABASE_NAME, props);

    }
}
