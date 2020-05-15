package model.faithbean;


import Base.OtherBaseEntity;

/**
 * <p>
 * 拍卖行
 * </p>
 *
 * @author twg
 * @since 2020-03-26
 */
public class ServerAuction extends OtherBaseEntity {

    public	String	sellerAccountId;
    public	String	sellerRoleGuid;
    public	String	sellerRoleName;
    public	Integer	sellerRoleLevel;
    public	String	itemGuid;
    public	String	itemId;
    public	Integer	itemNum;
    public	Integer	auctionType;
    public	String	buyerAccountId;
    public	String	buyerRoleGuid;
    public	String	buyerRoleName;
    public	Integer	buyerRoleLevel;
    public	Integer	moneyType;
    public	Integer	moneyValue;
    public	Integer	auctionState;
	public	String	currencyName;
	
}
