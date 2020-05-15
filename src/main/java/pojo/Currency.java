package pojo;

public class Currency {
    //事件名称
    private String event;
    //返回状态
    private String type;
    //用户的账号
    private String loginNumber;
    //
    private String strArr;
    //获取时间
    private String time;

    public String getEvent() {
        return event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getLoginNumber() {
        return loginNumber;
    }

    public void setLoginNumber(String loginNumber) {
        this.loginNumber = loginNumber;
    }

    public String getStrArr() {
        return strArr;
    }

    public void setStrArr(String strArr) {
        this.strArr = strArr;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }
}
