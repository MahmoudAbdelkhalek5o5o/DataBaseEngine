import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.TreeMap;
import java.util.Vector;

public class DBApp implements DBAppInterface {

	private int maxRowsInPage;
	private int maxKeysInIndexBucket;
	private ArrayList<String> tableNamesAddress;
	private Table currentTable;
	public Hashtable<String, Vector<String[]>> tableXHasGrids;
	static public GridIndex testgrid = null;
	static public boolean deleteRows = false;

	public DBApp() {
		init();
	}

	@Override
	public void init() {
		// initialization of the engine
		try {
			readConfig();
			File f = new File("src\\main\\resources\\data");
			f.mkdir();
			tableNamesAddress = new ArrayList<String>();
			tableXHasGrids = new Hashtable<String, Vector<String[]>>();
//			serializeGridInfo();
			createCSVHeaders(false);

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private void readConfig() {

		// reading the config file while tells the number of max rows and max indeces
		// per page
		Properties prop = new Properties();
		String fileName = "src\\main\\resources\\DBApp.config";
		FileInputStream is = null;
		try {
			is = new FileInputStream(fileName);
		} catch (FileNotFoundException ex) {

		}
		try {
			prop.load(is);
		} catch (IOException ex) {

		}

		maxRowsInPage = Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
		maxKeysInIndexBucket = Integer.parseInt(prop.getProperty("MaximumKeysCountinIndexBucket"));
	}

	private void createCSVHeaders(boolean overwrite) throws IOException {
		// create the table headers in the csv file
		File file = new File("src\\main\\resources\\metadata.csv");
		if (file.length() == 0 || overwrite) {
			FileWriter csvWriter = new FileWriter("src\\main\\resources\\metadata.csv");
			csvWriter.append("Table Name");
			csvWriter.append(",");
			csvWriter.append("Column Name");
			csvWriter.append(",");
			csvWriter.append("Column Type");
			csvWriter.append(",");
			csvWriter.append("ClusteringKey");
			csvWriter.append(",");
			csvWriter.append("Indexed");
			csvWriter.append(",");
			csvWriter.append("min");
			csvWriter.append(",");
			csvWriter.append("max");
			csvWriter.append("\n");
			csvWriter.flush();
			csvWriter.close();

		}
	}

	private void serializeTN() {
		try {

			FileOutputStream fileOut = new FileOutputStream("src\\main\\resources\\data\\engine_info");

			ObjectOutputStream out = new ObjectOutputStream(fileOut);

			out.writeObject(tableNamesAddress);

			out.close();

			fileOut.close();

		} catch (IOException i) {
			i.printStackTrace();
		}
	}

	private ArrayList<String> decerializeTN() {
		// TN for table names
		ArrayList<String> TN = null;
		try {
			File file = new File("src\\main\\resources\\data\\engine_info");
			FileInputStream fileIn;
			if (file.exists()) {
				fileIn = new FileInputStream("src\\main\\resources\\data\\engine_info");
			} else {
				return null;
			}

			ObjectInputStream in = new ObjectInputStream(fileIn);
			TN = (ArrayList<String>) in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException | ClassNotFoundException i) {
			i.printStackTrace();
			return null;
		}
		return TN;
	}

	private void serializeTable(Table TName) {
		try {

			FileOutputStream fileOut = new FileOutputStream("src\\main\\resources\\data\\" + TName.name + ".full");

			ObjectOutputStream out = new ObjectOutputStream(fileOut);

			out.writeObject(TName);

			out.close();

			fileOut.close();

		} catch (IOException i) {
			i.printStackTrace();
		}
	}

	private Table decerializeTable(String tableName) {
		Table table = null;
		try {
			FileInputStream fileIn = new FileInputStream("src\\main\\resources\\data\\" + tableName + ".full");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			table = (Table) in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException | ClassNotFoundException i) {
			return null;
		}
		return table;
	}

	private void serializeGridInfo() {
		try {

			FileOutputStream fileOut = new FileOutputStream("src\\main\\resources\\data\\gridsInfo.full");

			ObjectOutputStream out = new ObjectOutputStream(fileOut);

			out.writeObject(tableXHasGrids);

			out.close();

			fileOut.close();

		} catch (IOException i) {
			i.printStackTrace();
		}
	}

	private Hashtable<String, Vector<String[]>> decerializeGridInfo() {
		Hashtable<String, Vector<String[]>> info = null;
		try {
			FileInputStream fileIn = new FileInputStream("src\\main\\resources\\data\\gridsInfo.full");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			info = (Hashtable<String, Vector<String[]>>) in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException | ClassNotFoundException i) {
			return null;
		}
		return info;
	}

	private void serializeGrid(GridIndex myGrid) {
		try {

			FileOutputStream fileOut = new FileOutputStream(
					"src\\main\\resources\\data\\" + myGrid.tableName + displayArray(myGrid.order));

			ObjectOutputStream out = new ObjectOutputStream(fileOut);

			out.writeObject(myGrid);

			out.close();

			fileOut.close();

		} catch (IOException i) {
			i.printStackTrace();
		}
	}

	private GridIndex decerializeGrid(String tableName, String[] order) {
		GridIndex grid = null;
		try {
			FileInputStream fileIn = new FileInputStream(
					"src\\main\\resources\\data\\" + tableName + displayArray(order));
			ObjectInputStream in = new ObjectInputStream(fileIn);
			grid = (GridIndex) in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException | ClassNotFoundException i) {
			return null;
		}
		return grid;
	}

	private String displayArray(Object[] x) {
		String s = "";
		for (Object i : x) {
			s += "_" + i;
		}
		return s;
	}

//------------------------------------ main methods/functionality

	@Override
	public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
			Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
		Table created = new Table(tableName, maxRowsInPage);
		FileWriter csvWriter = null;
		try {
			csvWriter = new FileWriter("src\\main\\resources\\metadata.csv", true);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		try {
			tableNamesAddress = decerializeTN();
			if (tableNamesAddress != null) {
				for (int i = 0; i < tableNamesAddress.size(); i++) {
					if (tableName.equals(tableNamesAddress.get(i))) {
						throw new DBAppException("There is an existing table with the same name");
					}
				}
			} else
				tableNamesAddress = new ArrayList<String>();

			// you need to make sure everything is correct before filling the csv file:
			// check all sizes are equal else exception d
			// check data type is right d
			// check min and max are of the data type d
			// check true and false
			// check clustering key d

			Enumeration<String> Name = colNameType.keys();

			if (colNameType.size() != colNameMin.size())
				throw new DBAppException("You haven't enterd the minimum for all columns or entered more values");
			else if (colNameType.size() != colNameMax.size())
				throw new DBAppException("You haven't enterd the maximum for all columns or entered more values");
			else if (!colNameType.containsKey(clusteringKey)) {
				throw new DBAppException("make sure the clustering key data type is entered");
			}
			int counterForCluster = 0;
			while (Name.hasMoreElements()) {
				String currentName = Name.nextElement();
				String currentType = colNameType.get(currentName);
				Object currentMin = "";
				Object currentMax = "";
				currentMin = colNameMin.get(currentName);
				currentMax = colNameMax.get(currentName);

				switch (currentType) {
				case "java.lang.Integer":
					try {
						Integer.parseInt(currentMin + "");

					} catch (Exception e) {
						throw new DBAppException(currentMin + " is not an Integer");
					}
					try {
						Integer.parseInt(currentMax + "");
					} catch (Exception e) {
						throw new DBAppException(currentMax + " is not an Integer");
					}
					break;
				case "java.lang.Double":
					try {
						Double.parseDouble(currentMin + "");
					} catch (Exception e) {
						throw new DBAppException(currentMin + " is not Double");
					}
					try {
						Double.parseDouble(currentMax + "");
					} catch (Exception e) {
						throw new DBAppException(currentMax + " is not Double");
					}
					break;
				case "java.util.Date":
					try {

						new SimpleDateFormat("yyyy-mm-dd").parse(currentMin + "");
					} catch (Exception e) {
						throw new DBAppException(currentMin + " is not a vaild date");
					}
					try {

						new SimpleDateFormat("yyyy-mm-dd").parse(currentMax + "");
					} catch (Exception e) {
						throw new DBAppException(currentMax + " is not a vaild date");
					}
					break;
				case "java.lang.String":
					// note that instances like "ZYQ" can exist so the order of letter isn't handled
//					checkStringFormat(currentMin + "");
//					checkStringFormat(currentMax + "");
					break;
				default:
					throw new DBAppException(currentType + " isn't int,double,String nor date");
				}
				csvWriter.append(tableName);
				csvWriter.append(",");
				csvWriter.append(currentName);
				csvWriter.append(",");
				csvWriter.append(currentType);
				csvWriter.append(",");

				if (clusteringKey == currentName) {
					csvWriter.append("True");
					created.clusteringKeyIndex = counterForCluster;
					created.clusterKeyType = currentType;
				} else
					csvWriter.append("False");
				counterForCluster++;
				csvWriter.append(",");
				csvWriter.append("False");
				csvWriter.append(",");

				csvWriter.append(currentMin + "");
				csvWriter.append(",");
				csvWriter.append(currentMax + "");
				csvWriter.append("\n");
			}
			tableNamesAddress.add(tableName);
			serializeTN();
			serializeTable(created);
			csvWriter.flush();
			csvWriter.close();
		} catch (IOException e) {
			throw new DBAppException("Cannot read metadata file");
		}
	}

	@Override
	public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
		// TODO Auto-generated method stub

		if (checkAllTablesNamesFor(tableName) == null) {
			throw new DBAppException("The table doesn't exist in the DB");
		}

		try {
			ArrayList<String[]> columnsInfo = getColumnInfo(tableName);
			// 2.validate the methods input are correct
			validateInputs(tableName, columnsInfo, colNameValue);

			// figure to which table we insert
			currentTable = decerializeTable(checkAllTablesNamesFor(tableName));

			ArrayList<String> record = new ArrayList<String>(columnsInfo.size() - 1);
			int clusteringKey = 0;
			String clusteringKeyDataType = null;
			for (int i = 0; i < columnsInfo.size(); i++) {
				if (columnsInfo.get(i)[0].equals(tableName)) {
					if (columnsInfo.get(i)[2].equals("java.util.Date"))
						record.add(processDate(colNameValue.get(columnsInfo.get(i)[1]) + ""));
					else
						record.add(colNameValue.get(columnsInfo.get(i)[1]) + "");
					if (columnsInfo.get(i)[3].equalsIgnoreCase("True")) {
						clusteringKey = i;
						clusteringKeyDataType = columnsInfo.get(i)[2];
//						break;
					}
				}
			}

			int pageNum = 0;
			if (currentTable.pageAddress.size() == 0) {
				pageNum = -1;// i.e. doesn't exist

			} else if (currentTable.pageAddress.size() == 1) {

				pageNum = 0;
				if (currentTable.count.get(0) < maxRowsInPage) {
					pageNum = 0;
				} else {

					if ((compare((colNameValue.get(columnsInfo.get(clusteringKey)[5]) + ""),
							currentTable.max.get(0))) < 0)
						pageNum = 0;

					else
						pageNum = 1;
				}
			}

			else {

				for (int i = 0; i < currentTable.pageAddress.size() - 1; i++) {
					// first check compare with min & max in single page i
					if (recordInBetween(columnsInfo.get(clusteringKey)[5], currentTable.max.get(i) + "",
							record.get(clusteringKey), clusteringKeyDataType)) {
						pageNum = i;
						break;
					} // compare the max of cuurent page with the min of next page
					else if (recordInBetween(currentTable.max.get(i) + "", currentTable.min.get(i + 1) + "",
							record.get(clusteringKey), clusteringKeyDataType)) {
						if (currentTable.count.get(i) < maxRowsInPage) { // if current page has place
							pageNum = i;
						} else {
							pageNum = i + 1;
						}
					}
					// else loop
					else {
						pageNum = i + 1;
					}

				}

			}

			Object[] gridState = currentTable.insertInPage(pageNum, record);
			System.out.println("returned is " + gridState[0] + " " + gridState[1]);
			ArrayList<String> shiftedRecord = (ArrayList<String>) gridState[1];
			serializeTable(currentTable);
			tableXHasGrids = decerializeGridInfo();
			Vector<String[]> tableGridInfo = (tableXHasGrids == null) ? (null) : (tableXHasGrids.get(tableName));
			if (tableGridInfo == null) {
				return;
			}
			ArrayList<String[]> tableInfo = getColumnInfo(tableName);
			for (int i = 0; i < tableGridInfo.size(); i++) {

				GridIndex grid = decerializeGrid(tableName, tableGridInfo.get(i));
				grid.insertRecord(record, currentTable.pageAddress.get((Integer) gridState[0]) + "",
						getIndexOfColumnInTable(getClusterName(tableInfo), tableInfo));
				if (shiftedRecord != null) {
					grid.updatePageNumber(
							shiftedRecord.get(getIndexOfColumnInTable(getClusterName(tableInfo), tableInfo)),
							shiftedRecord, currentTable.pageAddress.get(((Integer) gridState[0]) + 1) + "");
				}
				testgrid = grid;
				serializeGrid(grid);
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue)
			throws DBAppException {

		if (checkAllTablesNamesFor(tableName) == null) {
			throw new DBAppException("The table doesn't exist in the DB");
		}

		ArrayList<String[]> columnsInfo = null;
		try {
			columnsInfo = getColumnInfo(tableName);
		} catch (IOException e) {

			e.printStackTrace();
		}
		if (!checkModCol(columnNameValue, columnsInfo)) {
			throw new DBAppException("The columns you want to update does not exist in the specified table");
		}

		for (int i = 0; i < columnsInfo.size(); i++) {
			if (!checkValueCompatibility(columnsInfo.get(i)[2], columnsInfo.get(i)[5], columnsInfo.get(i)[6],
					columnNameValue.get(columnsInfo.get(i)[1]) + "")) {
				throw new DBAppException("not compatible data type in column " + columnsInfo.get(i)[1]);
			}
		}

		// specify which table
		currentTable = decerializeTable(tableName);
		// specify type of clustering key
		String clusterType = getClusterType(columnsInfo);
		String clusterName = getClusterName(columnsInfo);
		if (columnNameValue.containsKey(clusterName)) {
			throw new DBAppException("You are not allowed to change the value of " + clusterName);
		}
		// Use The Index to update
		tableXHasGrids = (decerializeGridInfo() == null) ? (new Hashtable<String, Vector<String[]>>())
				: (decerializeGridInfo());

		if (tableXHasGrids.contains(tableName)) {
			SQLTerm[] sqlTerms = new SQLTerm[1];
			sqlTerms[0]._objValue = clusteringKeyValue;
			sqlTerms[0]._strTableName = tableName;
			sqlTerms[0]._strOperator = "=";
			sqlTerms[0]._strColumnName = clusterName;
			String[] arrayOperators = new String[0];
			Iterator<ArrayList<String>> selectedToBeUpdated = selectFromTable(sqlTerms, arrayOperators);
			ArrayList<String> row = selectedToBeUpdated.next();
			for (int i = 0; i < currentTable.pageAddress.size(); i++) {
				if (Table.compare(clusteringKeyValue, currentTable.min.get(i), clusterType) >= 0
						&& Table.compare(clusteringKeyValue, currentTable.max.get(i), clusterType) < 0) {
					Vector<ArrayList<String>> page = currentTable.decerialize(currentTable.pageAddress.get(i));
					int index = currentTable.binarySearch(page, clusteringKeyValue, clusterType)[0];
					if (index == -1 )
						throw new DBAppException("There is no such a record");
					ArrayList<String> oldRecord = new ArrayList<String>();

					for (int j = 0; j < page.get(index).size(); j++) {
						oldRecord.add(page.get(index).get(j));
					}
					int c = 0;
					for (int k = 0; k < columnsInfo.size(); k++) {
						String value = "" + columnNameValue.get(columnsInfo.get(k)[1]);
						if (value.equals("null")) {
							c--;
							continue;
						} else {
							page.get(index).set(k + c, value);
						}
					}
					currentTable.serialize(page, currentTable.pageAddress.get(i));
					Vector<String[]> tableGridInfo = tableXHasGrids.get(tableName);
					if (tableGridInfo == null) {
						continue;
					}
					for (int k = 0; k < tableGridInfo.size(); k++) {
						GridIndex grid = decerializeGrid(tableName, tableGridInfo.get(k));
						grid.updateRecord(clusteringKeyValue, row, oldRecord,
								getIndexOfColumnInTable(clusterName, columnsInfo),
								"" + currentTable.pageAddress.get(i));
						testgrid = grid;
						serializeGrid(grid);
					}
					break;
				}
				if (i==currentTable.pageAddress.size()-1)
					throw new DBAppException("There is no such a record");
			}
			serializeTable(currentTable);
			return;
		}

		// find page with the record
		Vector<ArrayList<String>> page = recordPage(currentTable, clusteringKeyValue, clusterType);
		if (page == null) {
			throw new DBAppException("There is no such a record");
		}
		// find index of the record in the specified page

		int recordIndex = currentTable.binarySearch(page, clusteringKeyValue, clusterType)[0];

		ArrayList<String> oldRecord = new ArrayList<String>();

		for (int i = 0; i < page.get(recordIndex).size(); i++) {
			oldRecord.add(page.get(recordIndex).get(i));
		}

		if (recordIndex == -1) {
			throw new DBAppException("There is no such a record");
		}
		int c = 0;
		for (int k = 0; k < columnsInfo.size(); k++) {
			String value = "" + columnNameValue.get(columnsInfo.get(k)[1]);
			if (value.equals("null")) {
				c--;
				continue;
			} else {
				page.get(recordIndex).set(k + c, value);
			}
		}
		int pageIndex = getPageIndex(currentTable, clusteringKeyValue, clusterType);
		ArrayList<String> record = page.get(recordIndex);
		currentTable.serialize(page, currentTable.pageAddress.get(pageIndex));
		serializeTable(currentTable);

		ArrayList<String> updatedColumns = new ArrayList<String>();

		columnNameValue.forEach((k, v) -> {
			updatedColumns.add(k);
		});

		// update grid
		try {
			tableXHasGrids = decerializeGridInfo();
			Vector<String[]> tableGridInfo = (tableXHasGrids == null) ? (null) : (tableXHasGrids.get(tableName));
			if (tableGridInfo == null) {
				return;
			}
			ArrayList<String[]> tableInfo = getColumnInfo(tableName);
			for (int i = 0; i < tableGridInfo.size(); i++) {
				if (checkIntersection(tableGridInfo.get(i), updatedColumns)) {
					GridIndex grid = decerializeGrid(tableName, tableGridInfo.get(i));

					grid.updateRecord(record.get(getIndexOfColumnInTable(clusterName, tableInfo)), record, oldRecord,
							getIndexOfColumnInTable(clusterName, tableInfo),
							currentTable.pageAddress.get(pageIndex) + "");
					System.out.println(grid);
					testgrid = grid;
					serializeGrid(grid);
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private boolean checkIntersection(String[] gridColumns, ArrayList<String> updatedColumns) {
		for (int i = 0; i < updatedColumns.size(); i++) {
			for (int j = 0; j < gridColumns.length; j++) {
				if (updatedColumns.get(i).equals(gridColumns[j])) {
					return true;
				}
			}
		}
		return false;
	}

	@Override
	public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
		try {
			if (checkAllTablesNamesFor(tableName) == null) {
				throw new DBAppException("The table doesn't exist in the DB");
			}

			ArrayList<String[]> columnsInfo = getColumnInfo(tableName);

			if (!checkModCol(columnNameValue, columnsInfo)) {
				throw new DBAppException("The columns you want to update does not exist in the specified table");
			}

			for (int i = 1; i < columnsInfo.size(); i++) {
				if (!checkValueCompatibility(columnsInfo.get(i)[2], columnsInfo.get(i)[5], columnsInfo.get(i)[6],
						columnNameValue.get(columnsInfo.get(i)[1]) + "")) {
					throw new DBAppException("not compatible data type in column " + columnsInfo.get(i)[1]);
				}
			} // specify which table

			currentTable = decerializeTable(tableName);
			String clusterName = getClusterName(columnsInfo);
			String clusterType = getClusterType(columnsInfo);
			int clusterIndex = getIndexOfColumnInTable(clusterName, getColumnInfo(tableName));
			String clusteringKeyValue = "" + columnNameValue.get(clusterName);
			int c = 0;
			// try
			tableXHasGrids = (decerializeGridInfo() == null) ? (new Hashtable<String, Vector<String[]>>())
					: (decerializeGridInfo());
			if (tableXHasGrids.contains(tableName)) {
				SQLTerm[] sqlTerms = new SQLTerm[columnNameValue.size()];
				int s = 0;
				Enumeration<String> columnNames = columnNameValue.keys();
				while (columnNames.hasMoreElements()) {
					String string = columnNames.nextElement();
					sqlTerms[s]._strTableName = tableName;
					sqlTerms[s]._strOperator = "=";
					sqlTerms[s]._strColumnName = string;
					sqlTerms[s]._objValue = columnNameValue.get(string);
					s++;
				}
				String[] arrayOperators = new String[sqlTerms.length - 1];
				for (int j = 0; j < arrayOperators.length; j++) {
					arrayOperators[j] = "and";
				}

				Iterator<ArrayList<String>> selectedToBeDeleted = selectFromTable(sqlTerms, arrayOperators);
				while (selectedToBeDeleted.hasNext()) {
					ArrayList<String> row = selectedToBeDeleted.next();
					String primaryKey = row.get(clusterIndex);
					for (int j = 0; j < currentTable.min.size(); j++) {
						if (Table.compare(primaryKey, currentTable.min.get(j), clusterType) >= 0
								&& Table.compare(primaryKey, currentTable.max.get(j), clusterType) < 0) {
							Vector<ArrayList<String>> page = currentTable.decerialize(currentTable.pageAddress.get(j));
							int index = currentTable.binarySearch(page, primaryKey, clusterType)[0];
							page.remove(index);
							currentTable.count.set(j, currentTable.count.get(j) - 1);
							if (page.isEmpty()) {
								currentTable.pageAddress.remove(j);
								currentTable.min.remove(j);
								currentTable.max.remove(j);
								currentTable.count.remove(j);
								j--;
							} else {
								currentTable.min.set(j, page.get(0).get(getIndexOfColumnInTable(clusterName, columnsInfo)));
								currentTable.max.set(j,
										page.get(page.size() - 1).get(getIndexOfColumnInTable(clusterName, columnsInfo)));
								currentTable.serialize(page, currentTable.pageAddress.get(j));
							}
							Vector<String[]> tableGridInfo = tableXHasGrids.get(tableName);
							if (tableGridInfo == null) {
								continue;
							}
							for (int k = 0; k < tableGridInfo.size(); k++) {
								GridIndex grid = decerializeGrid(tableName, tableGridInfo.get(k));
								grid.deleteRecord(primaryKey, row);
								testgrid = grid;
								serializeGrid(grid);
							}
						}
					}

				}
				serializeTable(currentTable);
				return;
			}
			// find page with the record

			if (clusteringKeyValue.equals("null")) {
				for (int i = 0; i < currentTable.pageAddress.size(); i++) {
					Vector<ArrayList<String>> page = currentTable.decerialize(currentTable.pageAddress.get(i));
					for (int j = 0; j < page.size(); j++) {
						if (equality(columnNameValue, page.get(j))) {
							c++;
							ArrayList<String[]> tableInfo = getColumnInfo(tableName);
							ArrayList<String> deletedRecord = page.get(j);
							String primaryKey = deletedRecord.get(getIndexOfColumnInTable(clusterName, tableInfo));
							page.remove(j);
							j--;
							currentTable.count.set(i, currentTable.count.get(i) - 1);
							tableXHasGrids = decerializeGridInfo();
							Vector<String[]> tableGridInfo = (tableXHasGrids == null) ? (null)
									: (tableXHasGrids.get(tableName));
							if (tableGridInfo == null) {
								continue;
							}
							tableInfo = getColumnInfo(tableName);
							for (int k = 0; k < tableGridInfo.size(); k++) {
								GridIndex grid = decerializeGrid(tableName, tableGridInfo.get(k));
								grid.deleteRecord(primaryKey, deletedRecord);
								testgrid = grid;
								serializeGrid(grid);

							}
						}
					}
					if (page.isEmpty()) {
						currentTable.pageAddress.remove(currentTable.pageAddress.get(i));
						currentTable.max.remove(i);
						currentTable.min.remove(i);
						currentTable.count.remove(i);
						i--;
					} else {
						currentTable.min.set(i, page.get(0).get(getIndexOfColumnInTable(clusterName, columnsInfo)));
						currentTable.max.set(i,
								page.get(page.size() - 1).get(getIndexOfColumnInTable(clusterName, columnsInfo)));
						currentTable.serialize(page, currentTable.pageAddress.get(i));
					}
				}
				if (c == 0) {
					throw new DBAppException("There is no such a record");
				} else {
//					System.out.println(c + " deleted Record/s");
				}
			} else {
				Vector<ArrayList<String>> page = recordPage(currentTable, clusteringKeyValue, clusterType);
				// page index is used to know the min and max value of the page of the cluster
				// key
				int pageIndex = getPageIndex(currentTable, clusteringKeyValue, clusterType);

				// index of the record in the specified page
				if (page == null) {
					throw new DBAppException("There is no such a record");
				}

				// find index of the record in the specified page

				int recordIndex = currentTable.binarySearch(page, clusteringKeyValue, clusterType)[0];
				if (recordIndex == -1) {
					throw new DBAppException("There is no such a record");
				}

				// used to update min and max of the page
				for (int i = 0; i < columnsInfo.size(); i++) {
					if (!(columnNameValue.get(columnsInfo.get(i)[1]) + "").equals(page.get(recordIndex).get(i))) {
						throw new DBAppException("There is no such a record");
					}
				}
				int oldPageSize = page.size();
				page.remove(recordIndex);
				currentTable.count.set(pageIndex, currentTable.count.get(pageIndex) - 1);
				if (page.isEmpty()) {
					currentTable.count.remove(pageIndex);
					currentTable.min.remove(pageIndex);
					currentTable.max.remove(pageIndex);
					currentTable.pageAddress.remove(pageIndex);

				}
				// check if the deleted record was the first or the last record in the page.
				// In other words, check if the clustering value of the record was minimum or
				// maximum value of the page
				else {
					if (recordIndex == oldPageSize - 1) {
						currentTable.max.set(pageIndex,
								page.get(page.size() - 1).get(getIndexOfColumnInTable(clusterName, columnsInfo)));
					} else {
						if (recordIndex == 0) {
							currentTable.min.set(pageIndex,
									page.get(0).get(getIndexOfColumnInTable(clusterName, columnsInfo)));
						}
					}
					currentTable.serialize(page, currentTable.pageAddress.get(pageIndex));
				}
			}
			serializeTable(currentTable);

		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

	@Override
	public void createIndex(String tableName, String[] columnNames) throws DBAppException {
		// TODO Auto-generated method stub
		if (checkAllTablesNamesFor(tableName) == null) {
			throw new DBAppException("table doesn't exist");
		}

		tableXHasGrids = decerializeGridInfo();
		if (tableXHasGrids == null)
			tableXHasGrids = new Hashtable<String, Vector<String[]>>();

		ArrayList<String[]> tableInfo = null;
		boolean thereExistIndexOnColumn = true;
		String wrong = "";
		try {
			tableInfo = getColumnInfo(tableName);
		} catch (IOException e) {
//			System.out.println("Invalid Table Name or Can't find the Path");
			e.printStackTrace();
		}
//		for (String desiredColomn : columnNames) {
//			thereExistIndexOnColumn = true;
//			for (String[] row : tableInfo) {
//				if (desiredColomn.equals(row[1]) && row[4].equalsIgnoreCase("False")) {
//					thereExistIndexOnColumn = false;
//
//					break;
//				}
//			}
//			if (thereExistIndexOnColumn) {
//				wrong = desiredColomn;
//				break;
//			}
//		}
		// decerializing avaialble grids
		if (tableXHasGrids.containsKey(tableName)) {
			Vector<String[]> availableGrids = tableXHasGrids.get(tableName);
			/*
			 * 1- if the input columns are subset of any grid: matches = columns length &
			 * columns length < grid size -> throw an exception
			 */

			/*
			 * 2- if subset of the input columns is a grid/grids :matches = Grid size & Grid
			 * size < columns length -> make a new grid and delete the old grids from disk
			 * and from the tableXHasGrids
			 */

			/*
			 * 3- if all the input columns exist in a grid: matches = columns length = Grid
			 * size - > reject
			 */

			/*
			 * 4- if subset of the input columns is subset of another grid : matches < grid
			 * size & matches < columns length -> create a grid and delete nothing
			 */
			// priorities 1>2>3>4
			int[] action = new int[availableGrids.size()];// based on the above cases, every grid has a case
			int finalAction = 4;
			for (int i = 0; i < availableGrids.size(); i++) {
				int matches = evaluateGrid(availableGrids.get(i), columnNames);
				// case 1
//				if (matches == columnNames.length && columnNames.length < availableGrids.get(i).length)
//					throw new DBAppException("similar or brroader grid exists");
				// case 3
				if (matches == columnNames.length && columnNames.length == availableGrids.get(i).length) {
					action[i] = 3;
					if (finalAction > 3)
						finalAction = 3;
				}
				// case 2
//				else if (matches == availableGrids.get(i).length && availableGrids.get(i).length < columnNames.length) {
//					action[i] = 2;
//					if (finalAction > 2)
//						finalAction = 2;
//				}
				// case 4
				else if (matches < availableGrids.get(i).length && matches < columnNames.length)
					action[i] = 4;// finalAction wouldn't get updated anyway
				else {
					action[i] = -1;
					finalAction = 4;// for debugging if errors happen, this shouldn't happen s
				}
			}
			if (finalAction == 500) {
				for (int i = action.length - 1; i >= 0; i--) {
					// delete all the smaller grids
					if (action[i] == 2) {
						availableGrids.remove(i);
						thereExistIndexOnColumn = false;
					}
				}
				tableXHasGrids.replace(tableName, availableGrids);
			} else if (finalAction == 3) {
				thereExistIndexOnColumn = true;
			} else if (finalAction == 4) {
				thereExistIndexOnColumn = false;
			} else {
				throw new DBAppException("look here");
			}

		} else {
			thereExistIndexOnColumn = false;
		}

		if (!thereExistIndexOnColumn) {
			System.out.println("EveryThing is good");
			for (String desiredColomn : columnNames) {
				try {
					ArrayList<String[]> data = getColumnInfo();
					ArrayList<String[]> data2 = getColumnInfo(tableName);

					boolean error = true;

					for (int j = 0; j < data2.size(); j++) {
						String CSVrow = data2.get(j)[1];
						if (desiredColomn.equals(CSVrow)) {
							error = false;
							break;
						}

					}
					if (error) {
						throw new DBAppException("no such a column idfsju");
					}

					for (int i = 1; i < data.size(); i++) {
						if (data.get(i)[0].equals(tableName) && data.get(i)[4].equalsIgnoreCase(("False"))
								&& data.get(i)[1].equals(desiredColomn)) {
							data.get(i)[4] = "True";
						}
					}

					createCSVHeaders(true);
					FileWriter csvWriter = new FileWriter("src\\main\\resources\\metadata.csv", true);
					for (int i = 1; i < data.size(); i++) {
						csvWriter.append(data.get(i)[0]);
						csvWriter.append(",");
						csvWriter.append(data.get(i)[1]);
						csvWriter.append(",");
						csvWriter.append(data.get(i)[2]);
						csvWriter.append(",");
						csvWriter.append(data.get(i)[3]);
						csvWriter.append(",");
						csvWriter.append(data.get(i)[4]);
						csvWriter.append(",");
						csvWriter.append(data.get(i)[5]);
						csvWriter.append(",");
						csvWriter.append(data.get(i)[6]);
						csvWriter.append("\n");
					}
					csvWriter.flush();
					csvWriter.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			try {
				ArrayList<String[]> data2 = getColumnInfo(tableName);
				String clusterColumn = getClusterName(data2);
				for (int i = 1; i < columnNames.length; i++) {
					if (clusterColumn.equals(columnNames[i])) {
						String temp = columnNames[0];
						columnNames[0] = clusterColumn;
						columnNames[i] = temp;
						break;
					}
				}
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			try {
				GridIndex grid = new GridIndex(columnNames, tableInfo, tableName,
						getColumnsOrder(tableName, columnNames), getColumnsType(tableName, columnNames),
						maxKeysInIndexBucket);
				currentTable = decerializeTable(tableName);
				for (int i = 0; i < currentTable.pageAddress.size(); i++) {
					Vector<ArrayList<String>> page = currentTable.decerialize(currentTable.pageAddress.get(i));
					grid.addData(page, currentTable.pageAddress.get(i) + "",
							getIndexOfColumnInTable(getClusterName(tableInfo), tableInfo));
					currentTable.serialize(page, currentTable.pageAddress.get(i));
				}
				serializeTable(currentTable);
				if (tableXHasGrids.containsKey(tableName)) {
					Vector<String[]> tmp = tableXHasGrids.get(tableName);
					tmp.add(columnNames);
					tableXHasGrids.replace(tableName, tmp);
				} else {
					Vector<String[]> tmp = new Vector<String[]>();
					tmp.add(columnNames);
					tableXHasGrids.put(tableName, tmp);
				}
				serializeGridInfo();
				serializeGrid(grid);

				System.out.println(grid);
				testgrid = grid;
			} catch (IOException e) {
				System.out.println("data mismatch");
				e.printStackTrace();
			}
		} else {
			throw new DBAppException(wrong + " colomn maybe be typed wrong or already has index");
		}
	}

	private int evaluateGrid(String[] inputGrid, String[] columnNames) {
		/*
		 * if all the input columns exist in a grid: matches = columns length = Grid
		 * size - > reject
		 */

		/*
		 * if the input columns are subset of any grid: matches = columns length &
		 * columns length < grid size -> throw an exception
		 */

		/*
		 * if subset of the input columns is a grid/grids :matches = Grid size & Grid
		 * size < columns length -> make a new grid and delete the old grids from disk
		 * and from the tableXHasGrids
		 */

		/*
		 * if subset of the input columns is subset of another grid : matches < grid
		 * size & matches < columns length -> create a grid and delete nothing
		 */

		int matchesNo = 0; // assuming nothing matches
		for (int i = 0; i < columnNames.length; i++) {

			for (int j = 0; j < inputGrid.length; j++)
				if (inputGrid[j].equals(columnNames[i])) {
					matchesNo++;
					break;
				}
		}
		return matchesNo;

	}

	private int[] getColumnsOrder(String tableName, String[] columnNames) throws IOException {

		// returns the columns order inside the csv file
		ArrayList<String[]> tableInfo = getColumnInfo(tableName);
		int[] order = new int[columnNames.length];
		for (int i = 0; i < columnNames.length; i++) {
			for (int j = 0; j < tableInfo.size(); j++) {
				if (columnNames[i].equals(tableInfo.get(j)[1])) {
					order[i] = j;
					break;
				}
			}
		}
		return order;
	}

	private String[] getColumnsType(String tableName, String[] columnNames) throws IOException {
		ArrayList<String[]> tableInfo = getColumnInfo(tableName);
		String[] dataType = new String[columnNames.length];
		for (int i = 0; i < columnNames.length; i++) {
			for (int j = 0; j < tableInfo.size(); j++) {
				if (columnNames[i].equals(tableInfo.get(j)[1])) {
					dataType[i] = tableInfo.get(j)[2];
					break;
				}
			}
		}
		return dataType;
	}

	private boolean acceptRow(String tableName, ArrayList<String> row, String columnName, String columnType,
			String value, String mathOp) {
		try {
			int index = getIndexOfColumnInTable(columnName, getColumnInfo(tableName));
			switch (mathOp) {
			case ">":
				if (Table.compare(row.get(index), value, columnType) > 0) {
					return true;
				}
				break;
			case ">=":
				if (Table.compare(row.get(index), value, columnType) >= 0) {
					return true;
				}
				break;

			case "=":
				if (Table.compare(row.get(index), value, columnType) == 0) {
					return true;
				}
				break;
			case "!=":
				if (Table.compare(row.get(index), value, columnType) != 0) {
					return true;
				}
				break;
			case "<":
				if (Table.compare(row.get(index), value, columnType) < 0) {
					return true;
				}
				break;
			case "<=":
				if (Table.compare(row.get(index), value, columnType) <= 0) {
					return true;
				}
				break;
			default:
				break;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return false;
	}

	private Vector<ArrayList<String>> andRowsFromPages(SQLTerm[] sqlTerms, Vector<ArrayList<String>> page,
			String[] columnType) {
		Vector<ArrayList<String>> acceptedRows = new Vector<ArrayList<String>>();
		for (int j = 0; j < page.size(); j++) {
			boolean flag = true;
			for (int k = 0; k < sqlTerms.length; k++) {
				if (!acceptRow(sqlTerms[0]._strTableName, page.get(j), sqlTerms[k]._strColumnName, columnType[k],
						sqlTerms[k]._objValue + "", sqlTerms[k]._strOperator)) {
					flag = false;
					break;
				}
			}
			if (flag) {
				acceptedRows.add(page.get(j));
			}
		}

		return acceptedRows;
	}

	private Vector<ArrayList<String>> orRowsFromPages(SQLTerm[] sqlTerms, Vector<ArrayList<String>> page,
			String[] columnType) {
		Vector<ArrayList<String>> acceptedRows = new Vector<ArrayList<String>>();

		for (int j = 0; j < page.size(); j++) {
			boolean flag = false;
			for (int k = 0; k < sqlTerms.length; k++) {
				if (acceptRow(sqlTerms[0]._strTableName, page.get(j), sqlTerms[k]._strColumnName, columnType[k],
						sqlTerms[k]._objValue + "", sqlTerms[k]._strOperator)) {
					flag = true;
					break;
				}
			}
			if (flag) {
				acceptedRows.add(page.get(j));
			}
		}

		return acceptedRows;
	}

	private Vector<ArrayList<String>> xorRowsFromPages(SQLTerm[] sqlTerms, Vector<ArrayList<String>> page,
			String[] columnType) {
		Vector<ArrayList<String>> acceptedRows = new Vector<ArrayList<String>>();
		for (int j = 0; j < page.size(); j++) {
			int flag = 0;
			for (int k = 0; k < sqlTerms.length; k++) {
				if (acceptRow(sqlTerms[0]._strTableName, page.get(j), sqlTerms[k]._strColumnName, columnType[k],
						sqlTerms[k]._objValue + "", sqlTerms[k]._strOperator)) {
					flag++;
					break;
				}
			}
			if (flag % 2 == 1) {
				acceptedRows.add(page.get(j));
			}
		}

		return acceptedRows;

	}

	@Override
	public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
		// TODO Auto-generated method stub

		if (checkAllTablesNamesFor(sqlTerms[0]._strTableName) == null) {
			throw new DBAppException("The table doesn't exist in the DB");
		}
		if (!validateLOperators(arrayOperators)) {
			throw new DBAppException("wrong logical operators");
		}

		for (int i = 0; i < sqlTerms.length; i++) {
			if (!(sqlTerms[i]._strOperator.equals(">")) && !(sqlTerms[i]._strOperator.equals(">="))
					&& !(sqlTerms[i]._strOperator.equals("<")) && !(sqlTerms[i]._strOperator.equals("<="))
					&& !(sqlTerms[i]._strOperator.equals("=")) && !(sqlTerms[i]._strOperator.equals("!="))) {
				throw new DBAppException("wrong mathematical operation");
			}
		}
		ArrayList<String[]> columnsInfo = new ArrayList<String[]>();
		try {
			boolean areEqual = false;
			columnsInfo = getColumnInfo(sqlTerms[0]._strTableName);
			for (int i = 0; i < sqlTerms.length; i++) {
				areEqual = false;
				for (int j = 0; j < columnsInfo.size(); j++) {
					if (sqlTerms[i]._strColumnName.equalsIgnoreCase(columnsInfo.get(j)[1])) {
						areEqual = true;
						break;

					}
				}
				if (!areEqual) {
					throw new DBAppException("column doesn't exist in the table");
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (int i = 0; i < columnsInfo.size(); i++) {
			for (int j = 0; j < sqlTerms.length; j++) {
				if (sqlTerms[j]._strColumnName.equals(columnsInfo.get(i)[1])) {
					if (!checkCompatibilitySelection(columnsInfo.get(i)[2], sqlTerms[j]._objValue + "")) {
						throw new DBAppException("not compatible datatype");
					}
				}
			}
		}
		String[] sqlColumns = new String[sqlTerms.length];
		for (int i = 0; i < sqlTerms.length; i++) {
			sqlColumns[i] = sqlTerms[i]._strColumnName;
		}
		tableXHasGrids = decerializeGridInfo();
		if (tableXHasGrids == null) {
			tableXHasGrids = new Hashtable<String, Vector<String[]>>();
		}
		Vector<String[]> availableGrids = tableXHasGrids.get(sqlTerms[0]._strTableName);
		Vector<String[]> possibleSubsetsVector = new Vector<String[]>();

		Vector<String> hasGrid = new Vector<String>();
		Vector<String> noGrid = new Vector<String>();

		// divide the query columns to those who have and those who don't have grid
		boolean exists = false;
		for (int i = 0; i < sqlColumns.length; i++) {
			exists = false;
			if (availableGrids != null) {

				for (int j = 0; j < availableGrids.size(); j++) {
					if (arrayContains(availableGrids.get(j), sqlColumns[i])) {
						exists = true;
						break;
					}
				}
			}
			if (exists)
				hasGrid.add(sqlColumns[i]);
			else// f a c //a c f //q: f a //f a, a f
				noGrid.add(sqlColumns[i]);
		}
		// get the available vectors that match the columns that have grids
		if (availableGrids != null) {
			for (int i = 0; i < availableGrids.size(); i++) {
				if (StringArrayToStringArrayList(availableGrids.get(i)).containsAll(hasGrid)) {
					if (possibleSubsetsVector.isEmpty()) {
						possibleSubsetsVector.add(availableGrids.get(i));
					} else {
						if (possibleSubsetsVector.get(0).length > availableGrids.get(i).length) {
							possibleSubsetsVector.set(0, availableGrids.get(i));
						}
					}

				}
			}
		}
//		else {
		String[] candidateGrid = null;
		// if there are a couple of possible grids, choose the minimum
		if (!possibleSubsetsVector.isEmpty()) {
			int index = 0;
			int min;
			// pick best grid with minimum length
			min = possibleSubsetsVector.get(0).length;
			for (int i = 1; i < possibleSubsetsVector.size(); i++) {
				if (min > possibleSubsetsVector.get(i).length) {
					min = possibleSubsetsVector.get(i).length;
					index = i;
				}
			}
			candidateGrid = availableGrids.get(index);
			for (int i = 0; i < candidateGrid.length; i++) {
				System.out.print(candidateGrid[i] + ",");
			}
//			possibleSubsetsVector.add(candidateGrid);
		} else {
			//
			while (!hasGrid.isEmpty()) {
				int[] matches = new int[availableGrids.size()];
				for (int i = 0; i < availableGrids.size(); i++) {
					matches[i] = matches(availableGrids.get(i), hasGrid);
				}
				int index = 0;
				int max = 0;
				;
				candidateGrid = null;
				// pick best grid with maximum match
				max = matches[0];
				for (int i = 1; i < matches.length; i++) {
					if (max < matches[i]) {
						max = matches[i];
						index = i;
					}
				}
				possibleSubsetsVector.add(availableGrids.get(index));
				hasGrid = subtract(hasGrid, availableGrids.get(index));
			}
			System.out.println("fdjsgvu");
		}
//		}
		for (int i = 0; i < possibleSubsetsVector.size(); i++) {
			for (int j = 0; j < possibleSubsetsVector.get(i).length; j++) {
				System.out.print(possibleSubsetsVector.get(i)[j] + ",");
			}
			System.out.println("");
		}
//		Vector<String[]> possibleSubsetsVector = findGridForDelete(sqlTerms[0]._strTableName, sqlColumns);

		GridIndex[] realGridAboutToBeUsed = new GridIndex[possibleSubsetsVector.size()];
		Vector<Vector<String>> columnsToBeUsedInMethod = new Vector<Vector<String>>();
		Vector<Vector<String>> mOperatorsToBeUsedInMethod = new Vector<Vector<String>>();
		Vector<Vector<Object>> valuesToBeUsedInMethod = new Vector<Vector<Object>>();

		for (int i = 0; i < possibleSubsetsVector.size(); i++) {
			realGridAboutToBeUsed[i] = decerializeGrid(sqlTerms[0]._strTableName, possibleSubsetsVector.get(i));
			Vector<String> tmpCol = new Vector<String>();
			Vector<String> tmpMO = new Vector<String>();
			Vector<Object> tmpValue = new Vector<Object>();
			for (int j = 0; j < possibleSubsetsVector.get(i).length; j++) {
				for (int k = 0; k < sqlTerms.length; k++) {
					if (possibleSubsetsVector.get(i)[j].equals(sqlTerms[k]._strColumnName)) {
//						boolean exists2 = false;
//						for (int l = 0; l < tmpCol.size(); l++) {
//							if (arrayContains(availableGrids.get(l), sqlTerms[k]._strColumnName)) {
//								exists2 = true;
//								break;
//							}
//						}
//						if (!exists2) {
						tmpCol.add(sqlTerms[k]._strColumnName);
						tmpMO.add(sqlTerms[k]._strOperator);
						tmpValue.add(sqlTerms[k]._objValue);
//						}
					}
				}
			}
			columnsToBeUsedInMethod.add(tmpCol);
			mOperatorsToBeUsedInMethod.add(tmpMO);
			valuesToBeUsedInMethod.add(tmpValue);
		}
		Vector<Vector<Bucket>> bucketsFromGrid = new Vector<Vector<Bucket>>();
		for (int i = 0; i < realGridAboutToBeUsed.length; i++) {
			String[] colNames = new String[columnsToBeUsedInMethod.get(i).size()];
			for (int j = 0; j < columnsToBeUsedInMethod.get(i).size(); j++) {
				colNames[j] = columnsToBeUsedInMethod.get(i).get(j);
			}

			try {

				bucketsFromGrid.add(realGridAboutToBeUsed[i].doLogicalOperation(realGridAboutToBeUsed[i].cells,
						columnsToBeUsedInMethod.get(i), getColumnsType(sqlTerms[0]._strTableName, colNames),
						valuesToBeUsedInMethod.get(i), mOperatorsToBeUsedInMethod.get(i), arrayOperators[0]));

				System.out.println(bucketsFromGrid);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		// rowsFromGridStart

		String tableName = sqlTerms[0]._strTableName;
		int tableNameLength = tableName.length();
		currentTable = decerializeTable(tableName);
		String[] columnNames = new String[sqlTerms.length];
		for (int i = 0; i < columnNames.length; i++) {
			columnNames[i] = sqlTerms[i]._strColumnName;
		}
		String[] columnType = new String[columnNames.length];
		try {
			columnType = getColumnsType(sqlTerms[0]._strTableName, columnNames);
		} catch (Exception e) {
			// TODO: handle exception
		}

		Vector<Vector<ArrayList<String>>> rowsFromGrid = new Vector<Vector<ArrayList<String>>>();

		for (int i = 0; i < bucketsFromGrid.size(); i++) {
			rowsFromGrid.add(new Vector<ArrayList<String>>());
		}
		for (int i = 0; i < bucketsFromGrid.size(); i++) {
			Vector<String> pageNamesFromGrid = uniquePages(bucketsFromGrid.get(i));
			Vector<Vector<String>> clusterFromGrid = getClusterFromGrid(bucketsFromGrid.get(i), pageNamesFromGrid);
			for (int j = 0; j < pageNamesFromGrid.size(); j++) {
				int order = Integer.parseInt(pageNamesFromGrid.get(j).substring(tableNameLength));
				Vector<ArrayList<String>> page = filterForGrid(currentTable.decerialize(order), clusterFromGrid.get(j));
				switch (arrayOperators[0]) {
				case "and":
					rowsFromGrid.get(i).addAll(andRowsFromPages(sqlTerms, page, columnType));
					break;
				case "or":
					rowsFromGrid.get(i).addAll(orRowsFromPages(sqlTerms, page, columnType));
					break;

				case "xor":
					rowsFromGrid.get(i).addAll(xorRowsFromPages(sqlTerms, page, columnType));
					break;
				default:
					break;
				}
			}
		}

		// now we need to do logical operation on rows that came from different grids
		// and rows that came from the original table

		/*
		 * TODO: here, first, get all the addresses from one grid that reference the
		 * same page, go open every page once and get the specified rows return the rows
		 * for each grid do the logical operations on them and the rest of non grid rows
		 * return the results in iterator
		 */

		// no grid part
		Vector<ArrayList<String>> rowsFromPages = null;
		if (!noGrid.isEmpty()) {
			rowsFromPages = new Vector<ArrayList<String>>();
			for (int i = 0; i < currentTable.pageAddress.size(); i++) {
				Vector<ArrayList<String>> page = currentTable.decerialize(currentTable.pageAddress.get(i));
				if (arrayOperators.length == 0) {
					rowsFromPages.addAll(andRowsFromPages(sqlTerms, page, columnType));
				} else {

					switch (arrayOperators[0]) {
					case "and":
						rowsFromPages.addAll(andRowsFromPages(sqlTerms, page, columnType));
						break;
					case "or":
						rowsFromPages.addAll(orRowsFromPages(sqlTerms, page, columnType));
						break;

					case "xor":
						rowsFromPages.addAll(xorRowsFromPages(sqlTerms, page, columnType));
						break;
					default:
						break;
					}
				}
			}
			for (int i = 0; i < rowsFromPages.size(); i++) {
				for (int j = 0; j < rowsFromPages.get(i).size(); j++) {
					System.out.print(rowsFromPages.get(i).get(j) + " ");
				}
				System.out.println();
			}
		}
		// now we need to do logical operation on rows that came from different grids
		// and rows that came from the original table

		Vector<Vector<ArrayList<String>>> allRows = rowsFromGrid;
		if (rowsFromPages != null)
			allRows.add(rowsFromPages);
		Vector<ArrayList<String>> rowsAfterFilter = new Vector<ArrayList<String>>();

		switch (arrayOperators[0]) {
		case "and":
			rowsAfterFilter = andSets(allRows);
			break;
		case "or":
			rowsAfterFilter = orSets(allRows);
			break;

		case "xor":
			rowsAfterFilter = xorSets(allRows);
			break;
		default:
			break;
		}

		final Vector<ArrayList<String>> finalReturnedRows = rowsAfterFilter;
		System.out.println(rowsAfterFilter);

		return new Iterator<ArrayList<String>>() {
			int i = 0;

			@Override
			public boolean hasNext() {
				// TODO Auto-generated method stub
				if (i == finalReturnedRows.size()) {
					return false;
				}
				return true;
			}

			@Override
			public ArrayList<String> next() {
				// TODO Auto-generated method stub

				if (this.hasNext()) {
					return finalReturnedRows.get(i++);
				}
				return null;
			}

		};
	}

	private Vector<ArrayList<String>> filterForGrid(Vector<ArrayList<String>> page, Vector<String> primaryKeysGrid) {
		ArrayList<String[]> columnsInfo = new ArrayList<String[]>();
		try {
			columnsInfo = getColumnInfo(currentTable.name);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Vector<ArrayList<String>> rowsFromGrid = new Vector<ArrayList<String>>();
		for (int i = 0; i < primaryKeysGrid.size(); i++) {

			int index = currentTable.binarySearch(page, primaryKeysGrid.get(i), getClusterType(columnsInfo))[0];
//			if (index == -1) {
//				continue;
//			}
//			System.out.println(primaryKeysGrid.get(i));
//			System.out.println(page);
//			System.out.println(i);
			rowsFromGrid.add(page.get(index));
		}

		return rowsFromGrid;
	}

	private Vector<Vector<String>> getClusterFromGrid(Vector<Bucket> gridBuckets, Vector<String> pageNamesFromGrid) {
		Vector<Vector<String>> clusterGrid = new Vector<Vector<String>>();
		for (int i = 0; i < pageNamesFromGrid.size(); i++) {
			clusterGrid.add(new Vector<String>());
		}
		for (int i = 0; i < gridBuckets.size(); i++) {
			gridBuckets.get(i).content.forEach((k, v) -> {
				clusterGrid.get(pageNamesFromGrid.indexOf(v)).add(k);
			});
		}
		return clusterGrid;
	}

	private Vector<ArrayList<String>> andSets(Vector<Vector<ArrayList<String>>> allRows) {
		while (allRows.size() > 1) {
			for (int i = 0; i < allRows.get(1).size(); i++) {
				if (!allRows.get(0).contains(allRows.get(1).get(i))) {
					allRows.get(1).remove(i);
				}
			}
			allRows.remove(0);
		}
		return allRows.get(0);
	}

	private Vector<ArrayList<String>> orSets(Vector<Vector<ArrayList<String>>> allRows) {
		while (allRows.size() > 1) {
			for (int i = 0; i < allRows.get(0).size(); i++) {
				if (!allRows.get(1).contains(allRows.get(0).get(i))) {
					allRows.get(1).add(allRows.get(0).get(i));
				}
			}
			allRows.remove(0);
		}
		return allRows.get(0);
	}

	private Vector<ArrayList<String>> xorSets(Vector<Vector<ArrayList<String>>> allRows) {
		while (allRows.size() > 1) {
			for (int i = 0; i < allRows.get(0).size(); i++) {
				if (!allRows.get(1).contains(allRows.get(0).get(i))) {
					allRows.get(1).add(allRows.get(0).get(i));
				} else {
					allRows.get(1).remove(allRows.get(0).get(i));
				}
			}
			allRows.remove(0);
		}
		return allRows.get(0);
	}

	private Vector<String> uniquePages(Vector<Bucket> gridBuckets) {
		Vector<String> pageNames = new Vector<String>();
		for (int i = 0; i < gridBuckets.size(); i++) {
			TreeMap<String, String> content = gridBuckets.get(i).content;
			content.forEach((k, v) -> {
				if (!pageNames.contains(v)) {
					pageNames.add(v);
				}
			});
		}
		return pageNames;
	}

	private Vector<String> subtract(Vector<String> remainder, String[] done) {
		for (int i = 0; i < done.length; i++) {
			remainder.remove(done[i]);
		}
		return remainder;
	}

	private int matches(String[] grid, Vector<String> sql) {
		int c = 0;
		for (int i = 0; i < sql.size(); i++) {
			for (int j = 0; j < grid.length; j++) {
				if (grid[j].equals(sql.get(i))) {
					c++;
					break;
				}
			}
		}

		return c;
	}

	private ArrayList<String> StringArrayToStringArrayList(String[] x) {
		ArrayList<String> y = new ArrayList<String>();
		for (int i = 0; i < x.length; i++) {
			y.add(x[i]);
		}
		return y;
	}

	private boolean arrayContains(String[] grid, String element) {
		for (int i = 0; i < grid.length; i++) {
			if (grid[i].equals(element)) {
				return true;
			}
		}
		return false;
	}

	private boolean checkCompatibilitySelection(String dataType, String enteredVal) {

		if (enteredVal.equals("null")) {
			return true;
		}

		switch (dataType) {
		case "java.lang.Integer":
			int valInt;
			try {
				valInt = Integer.parseInt(enteredVal);
			} catch (Exception e) {
				return false;
			}
			break;
		case "java.lang.Double":
			double valDouble;
			try {

				valDouble = Double.parseDouble(enteredVal);
			} catch (Exception e) {
				return false;
			}
			break;
		case "java.util.Date":
			String valDate;
			try {
				valDate = processDate(enteredVal);
			} catch (Exception e) {
				return false;
			}
			break;
		default:
		}

		return true;
	}

	public boolean validateLOperators(String[] arrayOperators) {
		for (int i = 0; i < arrayOperators.length; i++) {
			if (!arrayOperators[i].toLowerCase().equals("and") && !arrayOperators[i].toLowerCase().equals("xor")
					&& !arrayOperators[i].toLowerCase().equals("or")) {
				return false;

			}
		}
		return true;
	}

	// --------------------------------- helper methods
	private boolean checkValueCompatibility(String dataType, String min, String max, String enteredVal) {

		if (enteredVal.equals("null")) {
			return true;
		}

		switch (dataType) {
		case "java.lang.Integer":
			int valInt;
			try {
				valInt = Integer.parseInt(enteredVal);
			} catch (Exception e) {
				return false;
			}
			int minInt = Integer.parseInt(min);
			int maxInt = Integer.parseInt(max);
			if (valInt > maxInt || valInt < minInt)
				return false;
			break;
		case "java.lang.Double":
			double valDouble;
			try {
				valDouble = Double.parseDouble(enteredVal);
			} catch (Exception e) {
				return false;
			}
			double minDouble = Double.parseDouble(min);
			double maxDouble = Double.parseDouble(max);
			if (valDouble > maxDouble || valDouble < minDouble)
				return false;
			break;
		case "java.util.Date":
			String valDate;
			try {
				valDate = processDate(enteredVal);
				if (max.compareTo(valDate) < 0 || min.compareTo(valDate) > 0)
					return false;

			} catch (Exception e) {
				return false;
			}
			break;
		default:
			// string case
			if (enteredVal.length() > max.length())
				return false;
		}

		return true;
	}

	private void validateInputs(String tableName, ArrayList<String[]> columnsInfo,
			Hashtable<String, Object> colNameValue) throws DBAppException {
		boolean clusteringKeyExists = false;
		for (int i = 0; i < columnsInfo.size(); i++) {
			if (columnsInfo.get(i)[0].equals(tableName)) {

				if (colNameValue.size() > columnsInfo.size())
					throw new DBAppException("the number of provided paramaters don't match that of the thable");
				// check if clustering key is available

				if (columnsInfo.get(i)[3].equalsIgnoreCase("True")) {
					if (colNameValue.containsKey(columnsInfo.get(i)[1]))
						clusteringKeyExists = true;
				}

				// a. the column names
				if (colNameValue.containsKey(columnsInfo.get(i)[1])) {
					// b. the value is compatible with the data type and the min/max
					if (!checkValueCompatibility(columnsInfo.get(i)[2], columnsInfo.get(i)[5], columnsInfo.get(i)[6],
							colNameValue.get(columnsInfo.get(i)[1]) + ""))
						throw new DBAppException("the entered data isn't compatible with the table.\n"
								+ "The entered data should be between " + columnsInfo.get(i)[5] + " and "
								+ columnsInfo.get(i)[6] + " and be of type " + columnsInfo.get(i)[2]);
				}
			}

		}
		if (!clusteringKeyExists)
			throw new DBAppException("clustering key isn't provided in input");
	}

	private String processDate(String s) {
		// "Mon Sep 28 00:00:00 EEST 1992" -> yyyy-MM-dd

		String y = s.substring(s.length() - 4, s.length());
		String d = s.substring(8, 10);
		String m = s.substring(4, 7);
		switch (s.substring(4, 7)) {
		case "Jan":
			m = "01";
			break;
		case "Feb":
			m = "02";
			break;
		case "Mar":
			m = "03";
			break;
		case "Apr":
			m = "04";
			break;
		case "May":
			m = "05";
			break;
		case "Jun":
			m = "06";
			break;
		case "Jul":
			m = "07";
			break;
		case "Aug":
			m = "08";
			break;
		case "Sep":
			m = "09";
			break;
		case "Oct":
			m = "10";
			break;
		case "Nov":
			m = "11";
			break;
		case "Dec":
			m = "12";
			break;
		}
		return y + "-" + m + "-" + d;
	}

	private ArrayList<String[]> getColumnInfo(String tableName) throws IOException {
		BufferedReader csvReader = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
		String row = "";

		ArrayList<String[]> columnsInfo = new ArrayList<String[]>();
		while ((row = csvReader.readLine()) != null) {
			String[] info = row.split(",");
			if (info[0].equals(tableName)) {
				columnsInfo.add(info);
			}
		}
		csvReader.close();
		return columnsInfo;
	}

	private ArrayList<String[]> getColumnInfo() throws IOException {
		BufferedReader csvReader = new BufferedReader(new FileReader("src\\main\\resources\\metadata.csv"));
		String row = "";

		ArrayList<String[]> columnsInfo = new ArrayList<String[]>();
		while ((row = csvReader.readLine()) != null) {
			String[] info = row.split(",");
			columnsInfo.add(info);
		}
		csvReader.close();
		return columnsInfo;
	}

	private Vector<ArrayList<String>> recordPage(Table table, String clusteringKeyValue, String clusterType) {
		Vector<ArrayList<String>> page = null;
		for (int i = 0; i < currentTable.pageAddress.size(); i++) {
			if (recordInBetween(currentTable.min.get(i), currentTable.max.get(i), clusteringKeyValue, clusterType)) {
				page = currentTable.decerialize(currentTable.pageAddress.get(i));
				break;
			}
		}
		return page;
	}

	private boolean recordInBetween(String min, String max, String data, String dataType) {

		switch (dataType) {
		case "java.lang.Integer":

			int dataInt = Integer.parseInt(data);
			int minInt = Integer.parseInt(min);
			int maxInt = Integer.parseInt(max);
			if (maxInt >= dataInt && dataInt >= minInt)
				return true;
			else
				return false;
		case "java.lang.Double":

			double dataDouble = Double.parseDouble(data);
			double minDouble = Double.parseDouble(min);
			double maxDouble = Double.parseDouble(max);
			if (maxDouble >= dataDouble && dataDouble >= minDouble)
				return true;
			else
				return false;

		case "java.util.Date":
			try {
				FileWriter myWriter = new FileWriter("filename.txt", true);
				myWriter.write(max + " " + min + " " + data);
				myWriter.append(System.lineSeparator());
				myWriter.append(System.lineSeparator());

				myWriter.close();
			} catch (Exception e) {

			}
			if (compare(max, data) >= 0 && compare(data, min) >= 0)
				return true;
			return false;

		default:
			// string case
			if (compare(max, data) >= 0 && compare(data, min) >= 0)
				return true;
			else
				return false;

		}
	}

	private int compare(String s, String ss) {

		while (s.length() > 1) {
			if (s.charAt(0) == '0')
				s = s.substring(1);
			else
				break;
		}
		while (ss.length() > 1) {
			if (ss.charAt(0) == '0')
				ss = ss.substring(1);
			else
				break;
		}
		if (s.length() == ss.length())
			return s.compareTo(ss);

		return (s.length() > ss.length()) ? 1 : -1;
	}

	private String checkAllTablesNamesFor(String s) {
		ArrayList<String> temp = decerializeTN();
		tableNamesAddress = (temp == null) ? (new ArrayList<String>()) : temp;
		for (int i = 0; i < tableNamesAddress.size(); i++) {
			if (tableNamesAddress.get(i).equals(s)) {
				tableNamesAddress = new ArrayList<String>();
				return s;
			}

		}
		tableNamesAddress = new ArrayList<String>();

		return null;
	}

	private boolean checkModCol(Hashtable<String, Object> columnNameValue, ArrayList<String[]> columnsInfo) {
		// check if updated, inserted rows are have the same columns as in the table
		ArrayList<String> tableCol = new ArrayList<String>();
		for (int i = 0; i < columnsInfo.size(); i++) {
			tableCol.add(columnsInfo.get(i)[1]);
		}
		Enumeration<String> updatedCol = columnNameValue.keys();
		while (updatedCol.hasMoreElements()) {
			String colName = updatedCol.nextElement();
			if (!tableCol.contains(colName)) {
				return false;
			}
		}
		return true;
	}

	private boolean equality(Hashtable<String, Object> columnNameValue, ArrayList<String> record) throws IOException {
		ArrayList<String[]> columnsinfo = getColumnInfo(currentTable.name);
		Enumeration<String> Keys = columnNameValue.keys();
		while (Keys.hasMoreElements()) {
			String key = Keys.nextElement();
//			String s1 = columnNameValue.get(key) + "";
//			String s2 = record.get(getIndexOfColumnInTable(key, columnsinfo));
			if (!((columnNameValue.get(key)) + "").equals(record.get(getIndexOfColumnInTable(key, columnsinfo)))) {
				return false;
			}
		}
		return true;
	}

	private int getIndexOfColumnInTable(String columnName, ArrayList<String[]> columnsinfo) {
		int index = 0;
		for (int i = 0; i < columnsinfo.size(); i++) {
			if (columnName.equals(columnsinfo.get(i)[1])) {
				index = i;
				break;
			}
		}
		return index;
	}

	private String getClusterName(ArrayList<String[]> columnsInfo) {
		String clusterName = null;
		for (int i = 0; i < columnsInfo.size(); i++) {
			if (columnsInfo.get(i)[3].equalsIgnoreCase("True")) {
				clusterName = columnsInfo.get(i)[1];
				break;
			}
		}
		return clusterName;
	}

	private String getClusterType(ArrayList<String[]> columnsInfo) {
		String clusterType = null;
		for (int i = 0; i < columnsInfo.size(); i++) {
			if (columnsInfo.get(i)[3].equalsIgnoreCase("True")) {
				clusterType = columnsInfo.get(i)[2];
				break;
			}
		}
		return clusterType;
	}

	private int getPageIndex(Table table, String clusteringKeyValue, String clusterType) {
		int i = 0;
		for (i = 0; i < currentTable.pageAddress.size(); i++) {
			if (recordInBetween(currentTable.min.get(i), currentTable.max.get(i), clusteringKeyValue, clusterType)) {
				break;
			}
		}
		return i;
	}

	private void clearIndex() throws IOException {
		ArrayList<String[]> data = getColumnInfo();

		createCSVHeaders(true);

		for (int i = 1; i < data.size(); i++) {
			FileWriter csvWriter = new FileWriter("src\\main\\resources\\metadata.csv", true);
			csvWriter.append(data.get(i)[0]);
			csvWriter.append(",");
			csvWriter.append(data.get(i)[1]);
			csvWriter.append(",");
			csvWriter.append(data.get(i)[2]);
			csvWriter.append(",");
			csvWriter.append(data.get(i)[3]);
			csvWriter.append(",");
			csvWriter.append("false");
			csvWriter.append(",");
			csvWriter.append(data.get(i)[5]);
			csvWriter.append(",");
			csvWriter.append(data.get(i)[6]);
			csvWriter.append("\n");
			csvWriter.flush();
			csvWriter.close();
		}
		// delete the index info file
		File index = new File("src\\main\\resources\\data\\gridsInfo.full");
		index.delete();

	}

	private void cleanStart() {

		File index = new File("src\\main\\resources\\data");
		String[] entries = index.list();
		for (String s : entries) {
			File currentFile = new File(index.getPath(), s);
			currentFile.delete();
		}
		index.delete();

		new File("src\\main\\resources\\metadata.csv").delete();

		tableNamesAddress.clear();
		init();

	}

	public void printAvailableIndeces() {
		Hashtable<String, Vector<String[]>> tmp = decerializeGridInfo();

		Enumeration<String> e = tmp.keys();
		while (e.hasMoreElements()) {
			String table0 = e.nextElement();
			Vector<String[]> tempGrids = tmp.get(table0);
			System.out.print("table " + table0 + " has ");
			for (int i = 0; i < tempGrids.size(); i++) {
				System.out.print("grid " + i + ": ");
				for (int j = 0; j < tempGrids.get(i).length; j++) {
					System.out.print(tempGrids.get(i)[j] + ",");
				}
			}
			System.out.println("");
		}

	}

	public static Vector<Bucket> subtractVectors(Vector<Bucket> v1, Vector<Bucket> v2) {

		Vector<Bucket> res = (Vector<Bucket>) v1.clone();
		for (int i = 0; i < v1.size(); i++) {
			for (int j = 0; j < v2.size(); j++) {
				Bucket b1 = v1.get(i);
				Bucket b2 = v2.get(j);
				if (b1.equals(b2)) {
					res.remove(i);
				}
			}
		}
		return res;
	}

	// -----------------------------------main method
	public static void main(String[] args) throws DBAppException, IOException {
		DBApp dpa = new DBApp();
	}

}