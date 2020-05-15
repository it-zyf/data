package pojo;

public class DataInfo {

    private String plantform; //平台是安卓还是苹果

    private String deviceBrand; //手机厂商

    private String systemModel; //手机型号

    private String systemLanguage; //手机当前系统语言

    private String systemVersion; //系统版本号

    private String systemId; //ID

    private String macAddr; //wlan mac地址

    private String screenWidth1; //屏幕分辨率

    private String screenHeight1; //屏幕分辨率

    private String batteryLevel; //电池电量

    private String networkStates; //网络状态，WiFi还是2G，3G，4G，还是没网(0 没网/1 WiFt/2 2G/3 3G/4 4G)

    private String isGPSEnabled; //GPS是否开启(0 关闭/1 开启)

    private String iPAddress; //公网IP地址

    private String coldStartTime1; //启动时长，单位毫秒

    private String memoryTotalInfo; //总内存

    private String memoryAvailableInfo; //可用内存

    private String cpuMax; //CPU最大频率

    private String cpuMin; //CPU最小频率

    private String cpuCur; //CPU当前频率

    private String cpuName; //CPU名字

    private String gpuRenderer; //GPU渲染器

    private String gpuVersion; //GPU版本号

    private String time; //接收时间

    private String region; //地区

    private String storageMemory; //存储内存

    public String getPlantform() {
        return plantform;
    }

    public void setPlantform(String plantform) {
        this.plantform = plantform;
    }

    public String getDeviceBrand() {
        return deviceBrand;
    }

    public void setDeviceBrand(String deviceBrand) {
        this.deviceBrand = deviceBrand;
    }

    public String getSystemModel() {
        return systemModel;
    }

    public void setSystemModel(String systemModel) {
        this.systemModel = systemModel;
    }

    public String getSystemLanguage() {
        return systemLanguage;
    }

    public void setSystemLanguage(String systemLanguage) {
        this.systemLanguage = systemLanguage;
    }

    public String getSystemVersion() {
        return systemVersion;
    }

    public void setSystemVersion(String systemVersion) {
        this.systemVersion = systemVersion;
    }

    public String getSystemId() {
        return systemId;
    }

    public void setSystemId(String systemId) {
        this.systemId = systemId;
    }

    public String getMacAddr() {
        return macAddr;
    }

    public void setMacAddr(String macAddr) {
        this.macAddr = macAddr;
    }

    public String getScreenWidth1() {
        return screenWidth1;
    }

    public void setScreenWidth1(String screenWidth1) {
        this.screenWidth1 = screenWidth1;
    }

    public String getScreenHeight1() {
        return screenHeight1;
    }

    public void setScreenHeight1(String screenHeight1) {
        this.screenHeight1 = screenHeight1;
    }

    public String getBatteryLevel() {
        return batteryLevel;
    }

    public void setBatteryLevel(String batteryLevel) {
        this.batteryLevel = batteryLevel;
    }

    public String getNetworkStates() {
        return networkStates;
    }

    public void setNetworkStates(String networkStates) {
        this.networkStates = networkStates;
    }

    public String getIsGPSEnabled() {
        return isGPSEnabled;
    }

    public void setIsGPSEnabled(String isGPSEnabled) {
        this.isGPSEnabled = isGPSEnabled;
    }

    public String getiPAddress() {
        return iPAddress;
    }

    public void setiPAddress(String iPAddress) {
        this.iPAddress = iPAddress;
    }

    public String getColdStartTime1() {
        return coldStartTime1;
    }

    public void setColdStartTime1(String coldStartTime1) {
        this.coldStartTime1 = coldStartTime1;
    }

    public String getMemoryTotalInfo() {
        return memoryTotalInfo;
    }

    public void setMemoryTotalInfo(String memoryTotalInfo) {
        this.memoryTotalInfo = memoryTotalInfo;
    }

    public String getMemoryAvailableInfo() {
        return memoryAvailableInfo;
    }

    public void setMemoryAvailableInfo(String memoryAvailableInfo) {
        this.memoryAvailableInfo = memoryAvailableInfo;
    }

    public String getCpuMax() {
        return cpuMax;
    }

    public void setCpuMax(String cpuMax) {
        this.cpuMax = cpuMax;
    }

    public String getCpuMin() {
        return cpuMin;
    }

    public void setCpuMin(String cpuMin) {
        this.cpuMin = cpuMin;
    }

    public String getCpuCur() {
        return cpuCur;
    }

    public void setCpuCur(String cpuCur) {
        this.cpuCur = cpuCur;
    }

    public String getCpuName() {
        return cpuName;
    }

    public void setCpuName(String cpuName) {
        this.cpuName = cpuName;
    }

    public String getGpuRenderer() {
        return gpuRenderer;
    }

    public void setGpuRenderer(String gpuRenderer) {
        this.gpuRenderer = gpuRenderer;
    }

    public String getGpuVersion() {
        return gpuVersion;
    }

    public void setGpuVersion(String gpuVersion) {
        this.gpuVersion = gpuVersion;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getStorageMemory() {
        return storageMemory;
    }

    public void setStorageMemory(String storageMemory) {
        this.storageMemory = storageMemory;
    }
}
