package model.faithbean;


import Base.OtherBaseEntity;

/**
 * <p>
 * 货币变更
 * </p>
 *
 * @author twg
 * @since 2020-03-26
 */
public class ServerCurrencyChange extends OtherBaseEntity {
    public	String	logName;
    public	String	currencyId;
    public	Integer	changeType;
    public	Integer	recharge;
    public	Integer	changeDescribe;
    public	Integer	changeCount;
    public	Integer	residueCount;
}
