using CsvHelper.Configuration.Attributes;
public class Record
{
  [Name("tpep_pickup_datetime")]
  public DateTime TpepPickupDatetime { get; set; }

  [Name("tpep_dropoff_datetime")]
  public DateTime TpepDropoffDatetime { get; set; }

  [Name("passenger_count")]
  public int PassengerCount { get; set; }

  [Name("trip_distance")]
  public decimal TripDistance { get; set; }

  [Name("store_and_fwd_flag")]
  public string StoreAndFwdFlag
  {
    get => _storeAndFwdFlag;
    set => _storeAndFwdFlag = value is "Y" ? "Yes" : "No";
  }
  private string _storeAndFwdFlag;

  [Name("PULocationID")]
  public int PULocationID { get; set; }

  [Name("DOLocationID")]
  public int DOLocationID { get; set; }

  [Name("fare_amount")]
  public decimal FareAmount { get; set; }

  [Name("tip_amount")]
  public decimal TipAmount { get; set; }

}