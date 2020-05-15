package model.faithbean;


import Base.OtherBaseEntity;

/**
 * <p>
 * 充值日志
 * </p>
 *
 * @author twg
 * @since 2020-03-26
 */
public class ServerRechargeSuccess extends OtherBaseEntity {

    public String logName;
    public Integer roleVip;
    public String gameOrderId;
    public String gameChannelOrderId;
    public Long orderAmount;
    public Long shareaMount;
    public Long noShareaMount;
    public Integer payId;
    public Integer loginType;
    public String rechargeChannel;
    public Long addJewel;
    public Long totalJewel;
    public Long currencyType;
    public Long itemId;
    public Integer isFirstRecharge;
    public String deviceModel;
}
