public class SQLTerm {
	String _strTableName;
	String _strColumnName;
	String _strOperator;
	Object _objValue;

	public SQLTerm() {
	}

	public SQLTerm(String _strTableName, String _strColumnName, String _strOperator, Object _objValue) {
		super();
		this._strTableName = _strTableName;
		this._strColumnName = _strColumnName;
		this._strOperator = _strOperator;
		this._objValue = _objValue;
	}

}