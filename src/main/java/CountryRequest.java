public class CountryRequest {

    public String countryName;
    public Integer requestCount = 0;

    public CountryRequest(String countryName, Integer requestCount) {
        this.countryName = countryName;
        this.requestCount = requestCount;
    }

    public void IncrementRequests() {
        this.requestCount++;
    }


}
