/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.vertica;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Random;

import com.google.common.collect.Lists;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator.ActivationListener;

import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.common.util.NameableThreadFactory;
import com.datatorrent.contrib.vertica.Batch;
import com.google.common.collect.Queues;
import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class TableDataGenerator extends BaseOperator implements InputOperator, ActivationListener<Context>
{
  public final transient DefaultOutputPort<Batch> randomBatchOutput = new DefaultOutputPort<Batch>();
  int numprods;
  Integer[] prodVersionArray;
  String DEFAULT_TIMEFILE = "Time.txt";
  int DEFAULT_NUMPRODKEYS = 6000;
  int DEFAULT_NUMSTOREKEYS = 25;
  int DEFAULT_NUMPROMOKEYS = 100;
  int DEFAULT_NUMVENDKEYS = 50;
  int DEFAULT_NUMCUSTKEYS = 5000;
  int DEFAULT_NUMEMPKEYS = 1000;
  int DEFAULT_NUMSALESFACTROWS = 500000;
  int DEFAULT_NUMORDERFACTROWS = 30000;
  int DEFAULT_NUMFILES = 1;
  int DEFAULT_RAND = 20177;
  String DEFAULT_NULL_VALUE = "";
  int DEFAULT_NUMWAREHOUSEKEYS = 10;
  int DEFAULT_NUMSHIPPINGKEYS = 10;
  int DEFAULT_NUMONLINEPAGEKEYS = 100;
  int DEFAULT_NUMCALLCENTERKEYS = 20;
  int DEFAULT_NUMONLINESALESFACTROWS = 500000;
  int DEFAULT_NUMINVENTORYFACTROWS = 30000;
  int tid, cid;
  String timefile = DEFAULT_TIMEFILE;
  int numprodkeys = DEFAULT_NUMPRODKEYS;
  int numstorekeys = DEFAULT_NUMSTOREKEYS;
  int numpromokeys = DEFAULT_NUMPROMOKEYS;
  int numvendkeys = DEFAULT_NUMVENDKEYS;
  int numcustkeys = DEFAULT_NUMCUSTKEYS;
  int numempkeys = DEFAULT_NUMEMPKEYS;
  int numfactsalesrows = DEFAULT_NUMSALESFACTROWS;
  int numfactorderrows = DEFAULT_NUMORDERFACTROWS;
  int numfiles = DEFAULT_NUMFILES;
  int rand = DEFAULT_RAND;
  String nullflag = DEFAULT_NULL_VALUE;
  int numwarehousekeys = DEFAULT_NUMWAREHOUSEKEYS;
  int numshippingkeys = DEFAULT_NUMSHIPPINGKEYS;
  int numonlinepagekeys = DEFAULT_NUMONLINEPAGEKEYS;
  int numcallcenterkeys = DEFAULT_NUMCALLCENTERKEYS;
  int numfactonlinesalesrows = DEFAULT_NUMONLINESALESFACTROWS;
  int numfactinventoryrows = DEFAULT_NUMINVENTORYFACTROWS;
  int NUM_LOC = 110;
  int numtimekeys;
  String[][] locations = new String[][] {
    {"New York", "NY", "East"},
    {"Los Angeles", "CA", "West"},
    {"Chicago", "IL", "MidWest"},
    {"Houston", "TX", "South"},
    {"Philadelphia", "PA", "East"},
    {"Phoenix", "AZ", "SouthWest"},
    {"San Diego", "CA", "West"},
    {"Dallas", "TX", "South"},
    {"San Antonio", "TX", "South"},
    {"Detroit", "MI", "MidWest"},
    {"San Jose", "CA", "West"},
    {"Indianapolis", "IN", "MidWest"},
    {"San Francisco", "CA", "West"},
    {"Jacksonville", "FL", "South"},
    {"Columbus", "OH", "MidWest"},
    {"Austin", "TX", "South"},
    {"Baltimore", "MD", "East"},
    {"Memphis", "TN", "East"},
    {"Milwaukee", "WI", "MidWest"},
    {"Boston", "MA", "East"},
    {"Las Vegas", "NV", "SouthWest"},
    {"Washington", "DC", "East"},
    {"Nashville", "TN", "East"},
    {"El Paso", "TX", "South"},
    {"Seattle", "WA", "NorthWest"},
    {"Denver", "CO", "SouthWest"},
    {"Charlotte", "NC", "East"},
    {"Fort Worth", "TX", "South"},
    {"Portland", "OR", "NorthWest"},
    {"Pasadena", "CA", "West"},
    {"Escondido", "CA", "West"},
    {"Sunnyvale", "CA", "West"},
    {"Savannah", "GA", "South"},
    {"Fontana", "CA", "West"},
    {"Orange", "CA", "West"},
    {"Naperville", "IL", "MidWest"},
    {"Alexandria", "VA", "East"},
    {"Rancho Cucamonga", "CA", "West"},
    {"Grand Prairie", "TX", "South"},
    {"Fullerton", "CA", "West"},
    {"Corona", "CA", "West"},
    {"Flint", "MI", "MidWest"},
    {"Mesquite", "TX", "South"},
    {"Sterling Heights", "MI", "East"},
    {"Sioux Falls", "SD", "MidWest"},
    {"New Haven", "CT", "East"},
    {"Topeka", "KS", "SouthWest"},
    {"Concord", "CA", "West"},
    {"Evansville", "IN", "MidWest"},
    {"Hartford", "CT", "East"},
    {"Fayetteville", "NC", "East"},
    {"Cedar Rapids", "IA", "MidWest"},
    {"Elizabeth", "NJ", "East"},
    {"Lansing", "MI", "MidWest"},
    {"Lancaster", "CA", "West"},
    {"Fort Collins", "CO", "SouthWest"},
    {"Coral Springs", "FL", "South"},
    {"Stamford", "CT", "East"},
    {"Thousand Oaks", "CA", "West"},
    {"Vallejo", "CA", "West"},
    {"Palmdale", "CA", "West"},
    {"Columbia", "SC", "East"},
    {"El Monte", "CA", "West"},
    {"Abilene", "TX", "South"},
    {"North Las Vegas", "NV", "SouthWest"},
    {"Ann Arbor", "MI", "MidWest"},
    {"Beaumont", "TX", "South"},
    {"Waco", "TX", "South"},
    {"Independence", "MS", "South"},
    {"Peoria", "IL", "MidWest"},
    {"Inglewood", "CA", "West"},
    {"Springfield", "IL", "MidWest"},
    {"Simi Valley", "CA", "West"},
    {"Lafayette", "LA", "South"},
    {"Gilbert", "AZ", "SouthWest"},
    {"Carrollton", "TX", "South"},
    {"Bellevue", "WA", "NorthWest"},
    {"West Valley City", "UT", "West"},
    {"Clearwater", "FL", "South"},
    {"Costa Mesa", "CA", "West"},
    {"Peoria", "AZ", "SouthWest"},
    {"South Bend", "IN", "MidWest"},
    {"Downey", "CA", "West"},
    {"Waterbury", "CT", "East"},
    {"Manchester", "NH", "East"},
    {"Allentown", "PA", "East"},
    {"McAllen", "TX", "South"},
    {"Joliet", "IL", "MidWest"},
    {"Lowell", "MA", "East"},
    {"Provo", "UT", "West"},
    {"West Covina", "CA", "West"},
    {"Wichita Falls", "TX", "South"},
    {"Erie", "PA", "East"},
    {"Daly City", "CA", "West"},
    {"Clarksville", "TN", "East"},
    {"Norwalk", "CA", "West"},
    {"Gary", "IN", "MidWest"},
    {"Berkeley", "CA", "West"},
    {"Santa Clara", "CA", "West"},
    {"Green Bay", "WI", "MidWest"},
    {"Cape Coral", "FL", "South"},
    {"Arvada", "CO", "SouthWest"},
    {"Pueblo", "CO", "SouthWest"},
    {"Athens", "GA", "South"},
    {"Cambridge", "MA", "East"},
    {"Westminster", "CO", "SouthWest"},
    {"Ventura", "CA", "West"},
    {"Portsmouth", "VA", "East"},
    {"Livonia", "MI", "MidWest"},
    {"Burbank", "CA", "West"}
  };
  String[] streets = new String[] {
    "Main St", "School St", "Mission St", "Railroad St", "Market St",
    "Bakers St", "Church St", "Essex St", "Brook St", "Lake St",
    "Elm St", "Maple St", "Pine St", "Hereford Rd", "Cherry St",
    "Alden Ave", "Humphrey St", "Davis Rd", "Green St", "Kelly St"
  };
  String[] mname = new String[] {
    "Seth", "Doug", "Duncan", "Joseph", "Thom",
    "Jose", "Sam", "Alexander", "David", "John",
    "Michael", "James", "Steve", "Kevin", "Luigi",
    "Ben", "Jack", "Robert", "Raja", "Harold",
    "Matt", "Lucas", "Marcus", "Dean", "Craig",
    "Theodore", "Brian", "Mark", "Daniel", "William"
  };
  String[] fname = new String[] {
    "Samantha", "Sarah", "Rebecca", "Sally", "Tanya",
    "Darlene", "Amy", "Jessica", "Linda", "Barbara",
    "Carla", "Juanita", "Sharon", "Mary", "Ruth",
    "Wendy", "Anna", "Laura", "Lauren", "Kim",
    "Emily", "Meghan", "Joanna", "Lily", "Martha",
    "Tiffany", "Alexandra", "Midori", "Betty", "Julie"
  };
  String[] midinit = new String[] {
    "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O",
    "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z", ""
  };
  String[] surname = new String[] {
    "Vu", "Li", "Smith", "Martin", "Roy",
    "Brown", "Williams", "Peterson", "Garnett", "Taylor",
    "Farmer", "Perkins", "Jefferson", "Gauthier", "Fortin",
    "Campbell", "Garcia", "Rodriguez", "Moore", "Sanchez",
    "Young", "Lewis", "Harris", "Weaver", "Nguyen",
    "Bauer", "Nielson", "Lampert", "Vogel", "Miller",
    "Wilson", "Robinson", "Carcetti", "Jones", "Goldberg",
    "Overstreet", "Meyer", "Webber", "King", "Pavlov",
    "Dobisz", "Lang", "Stein", "McCabe", "McNulty",
    "Kramer", "Winkler", "Greenwood", "Jackson", "Reyes"
  };
  String[] title = new String[] {"Mr.", "Sir", "Dr.", "Mrs.", "Ms."};
  private volatile boolean generate = true;
  protected transient ExecutorService executorService;
  private Queue<Batch> queue = Queues.newLinkedBlockingQueue();

  @Override
  public void activate(Context context)
  {
    executorService = Executors.newSingleThreadExecutor(new NameableThreadFactory("RowGeneration-Helper"));
    executorService.submit(generateRows());
  }

  @Override
  public void deactivate()
  {
    generate = false;
    executorService.shutdown();
  }

  private Runnable generateRows()
  {
    return new Runnable()
    {
      @Override
      public void run()
      {
        numtimekeys = time_gen("timeTable", timefile);
        while (generate) {
          store_gen("store.store_dimension", numstorekeys);
          //int numtimekeys = time_gen("xxx", "Date_Dimension");
          prod_gen("product_dimension", numprodkeys);
          promo_gen("promotion_dimension", numpromokeys);
          vend_gen("vendor_dimension", numvendkeys);
          cust_gen("customer_dimension", numcustkeys);
          emp_gen("employee_dimension", numempkeys);
          warehouse_gen("warehouse_dimension", numwarehousekeys);
          shipping_gen("shipping_dimension", numshippingkeys);
          onlinepage_gen("online_sales.online_page_dimension", numonlinepagekeys);
          callcenter_gen("online_sales.call_center_dimension", numcallcenterkeys);

          salesfact_gen("store.store_sales_fact", numfactsalesrows);
          storeorder_gen("store.store_orders_fact", numfactorderrows);
          inventoryfact_gen("Inventory_Fact", numfactinventoryrows);
          onlinesalesfact_gen("online_sales.online_sales_fact", numfactonlinesalesrows);
          generate = false;
        }
      }

    };
  }

  @Override
  public void emitTuples()
  {
    if (!queue.isEmpty()) {
      Batch batch = queue.poll();
      randomBatchOutput.emit(batch);
    }
  }

  private void prod_gen(String tableName, int numproducts)
  {
    int NUM_PRODS = 16;
    String[] dept = new String[] {
      "Frozen Goods",
      "Produce",
      "Bakery",
      "Canned Goods",
      "Meat",
      "Dairy",
      "Seafood",
      "Liquor",
      "Cleaning supplies",
      "Medical",
      "Pharmacy",
      "Photography",
      "Gifts"
    };

    String[][] prod = new String[][] { // frozen goods
      {"frozen pizza", "frozen ravioli", "chocolate ice cream",
       "vanilla ice cream", "strawberry ice cream", "frozen juice",
       "fish sticks", "frozen stuffed chicken", "frozen french fries",
       "ice cream sandwhiches", "tv dinner", "frozen bagels",
       "frozen chicken patties", "frozen spinach", "frozen onion rings", "ice"
      },
      // produce
      {"apples", "bananas", "watermelon", "strawberries",
       "tomatoes", "broccolli", "lettuce", "potatoes",
       "red peppers", "green peppers", "cucumbers", "oranges",
       "onions", "cantaloupe", "squash", "garlic"
      },
      // bakery
      {"french bread", "coffee cake", "hamburger buns", "hotdog buns",
       "english muffins", "blueberry muffins", "corn muffins", "croissants",
       "jelly donuts", "chocolate chip cookies", "peanut butter cookies", "bagels",
       "italian bread", "wheat bread", "white bread", "cinnamon buns"
      },
      // canned goods
      {"chicken noodle soup", "vegatable soup", "minestrone soup", "canned tuna",
       "canned chili", "canned tomatoes", "baked beans", "kidney beans",
       "fruit cocktail", "canned peaches", "canned corn", "canned olives",
       "canned green beans", "lima beans", "sardines", "canned chicken broth"
      },
      // meat
      {"steak", "tenderloin", "lamb", "ground beef",
       "sausage", "sliced turkey", "glazed ham", "rotisserie chicken",
       "chicken nuggets", "chicken patties", "veal", "pork",
       "bacon", "hot dogs", "chicken wings", "bratwurst"
      },
      // dairy
      {"whole milk", "low fat milk", "eggs", "butter",
       "cheddar cheese", "mozzarella cheese", "american cheese", "yogurt",
       "whipped cream", "skim milk", "lactose free milk", "half and half",
       "grated parmesan cheese", "sour cream", "margarine", "butter milk"
      },
      // seafood
      {"shrimp", "lobster", "salmon", "tuna",
       "cod", "halibut", "swordfish", "sushi",
       "fried fish patties", "crab cakes", "scallops", "clams",
       "crab legs", "flounder", "haddock", "catfish"
      },
      // liquor
      {"beer", "red wine", "white wine", "vodka",
       "whiskey", "scotch", "tequila", "gin",
       "rum", "brandy", "bourbon", "champagne",
       "light beer", "ale", "hard lemonade", "hard iced tea"
      },
      // cleaning supplies
      {"dish soap", "brooms", "mops", "laundry detergent",
       "fabric softener", "air freshener", "bleach", "sponges",
       "paper towels", "trash bags", "oven cleaner", "glass cleaner",
       "rubber gloves", "feather duster", "starch", "silver polishing cream"
      },
      // medical
      {"band aids", "baby powder", "first aid kit", "sling",
       "wrap bandage", "crutches", "cold remedy", "thermometer",
       "nasal spray", "air humidifier", "air purifier", "splint",
       "wheechair", "rash ointment", "heating pad", "orthopedic brace"
      },
      // pharmacy
      {"antibiotics", "allergy pills", "pain killers", "asthma inhaler",
       "sleeping pills", "nicotine gum", "nicotine patches", "diabetes blood testing kit",
       "cough syrup", "blood pressure medicine", "cholesterol reducer", "birth control",
       "vapor rub", "ulcer medication", "decongestant", "heart medication"
      },
      // photography
      {"digital camera", "telephoto lens", "film", "memory card",
       "analog camera", "camera case", "camera strap", "photography paper",
       "camera batteries", "picture frames", "photo albums", "lens cap",
       "cables", "adapters", "camera cleaner kit", "lens cleaner"
      },
      // gifts
      {"greeting cards", "birthday cards", "watch", "ring",
       "bracelet", "box of candy", "stuffed animal", "flowers",
       "perfume", "electric razor", "beard trimmer", "football",
       "basketball", "tennis racket", "fishing pole", "golf clubs"
      }
    };
    String[] cat = new String[] {
      "Food",
      "Food",
      "Food",
      "Food",
      "Food",
      "Food",
      "Food",
      "Non-food",
      "Non-food",
      "Medical",
      "Medical",
      "Misc",
      "Misc"
    };


    String[] package1 = new String[] {
      "Bottle",
      "Box",
      "Bag",
      "Other"
    };
    String[] pkgsize = new String[] {
      "12 oz",
      "18 oz",
      "1 litre",
      "1 gallon",
      "Economy",
      "Family",
      "Other",};
    String[] diet = new String[] {
      "Atkins",
      "Weight Watchers",
      "Light",
      "Low Fat",
      "South Beach",
      "Zone",
      "N/A"
    };
    String[] units = new String[] {"pound", "ounce", "gram"};

    // printf("Product_Dimension(product_key|description|sku|cat|dept|package|pkgsize|fat|diet|wt|wtunits|shelf_width|shelf_ht|shelf_depth)\n");

    prodVersionArray = new Integer[numproducts];

    int prodkey = 1;
    int prodversion = 1;

    ArrayList<String[]> rows = Lists.newArrayList();
    for (int i = 1; i <= numproducts; i++) {
      int d = randInt(0, 12);

      if (prodversion == 1) {
        prodversion = randInt(1, 5);
        if (i != 1) {
          prodkey++;
        }
        prodVersionArray[prodkey] = prodversion;
      }
      else {
        prodversion--;
      }

      List<String> row = Lists.newArrayList();

      row.add(String.format("%d", prodkey));
      row.add(String.format("%d", prodversion));
      row.add(String.format("Brand #%d %s", i, prod[d][randInt(0, NUM_PRODS - 1)]));
      row.add(String.format("SKU-#%d", i));
      row.add(String.format("%s", cat[d]));
      row.add(String.format("%s", dept[d]));

      int NUM_FOODS = 7;
      if (d < NUM_FOODS) {
        row.add(String.format("%s", package1[randInt(0, 3)]));
        row.add(String.format("%s", pkgsize[randInt(0, 6)]));
        row.add(String.format("%d", randInt(80, 90)));
        row.add(String.format("%s", diet[randInt(0, 6)]));
        row.add(String.format("%d", randInt(1, 100)));
        row.add(String.format("%s", units[randInt(0, 2)]));
      }
      else {
        row.add(String.format("Other"));
        row.add(String.format("Other"));
        row.add(String.format("0"));
        row.add(String.format("N/A"));
        row.add(String.format("%d", randInt(1, 100)));
        row.add(String.format("pound"));
      }
      row.add(String.format("%d", randInt(1, 5)));
      row.add(String.format("%d", randInt(1, 5)));
      row.add(String.format("%d", randInt(1, 5)));

      int cost = randInt(1, 300);
      int profit = randInt(5, 300);
      int price = cost + profit;
      int lcp = price - randInt(0, price / 10);
      int hcp = price + randInt(0, price / 10);
      int acp = (lcp + hcp) / 2;

      row.add(String.format("%d", price)); // product price
      row.add(String.format("%d", cost)); // product cost
      row.add(String.format("%d", lcp)); // lowest competitor price
      row.add(String.format("%d", hcp)); // highest competitor price
      row.add(String.format("%d", acp)); // average competitor price

      int disc = randInt(1, 50); //discontinued item (only 1 out of 50)
      if (disc > 1) {
        row.add(String.format("%d", 0));
      }
      else {
        row.add(String.format("%d", 1));
      }

      rows.add(row.toArray(new String[row.size()]));
    }
    prodVersionArray[prodkey] = prodversion;
    numprods = prodkey - 1;

    Batch batch = new Batch();
    batch.tableName = tableName;
    batch.rows = rows;

    queue.add(batch);
  }

  private void store_gen(String tableName, int numstores)
  {
    String[] photoTypes = new String[] {"Premium", "1 hr", "24 hr", "DIY", "None"};
    String[] finTypes = new String[] {"Bank", "ATM", "CheckCashing", "Mortgage", "None"};


    // printf("Store_Dimension(store_key,store_name,store_number,street_address,city,state,region,floor_plan,photo,financial_srv,sell_sq_footage,sq_footage,first-open, last-remodel\n");

    ArrayList<String[]> rows = Lists.newArrayList();
    for (int i = 1; i <= numstores; i++) {
      List<String> row = Lists.newArrayList();
      row.add(String.format("%d", i));
      row.add(String.format("Store%d", i));
      row.add(String.format("%d", i));
      row.add(String.format("%d %s", randInt(0, 500), streets[randInt(0, 19)]));

      int loc = randInt(0, NUM_LOC - 1);

      row.add(String.format("%s", locations[loc][0]));
      row.add(String.format("%s", locations[loc][1]));
      row.add(String.format("%s", locations[loc][2]));
      row.add(String.format("Plan%d", i % 5));
      row.add(String.format("%s", photoTypes[randInt(0, 4)]));
      row.add(String.format("%s", finTypes[randInt(0, 4)]));

      int sqfoot = randInt(1, 10) * 1000;
      row.add(String.format("%d", (int)(sqfoot * randInt(1, 5) * 0.1)));
      row.add(String.format("%d", sqfoot));

      int mmonth = randInt(1, 6);
      int myear = randInt(2003, 2007);
      int mday = 1;

      row.add(String.format("%d-%d-%d", myear, mmonth, mday));

      int remodel = randInt(1, 4);

      if (myear + remodel <= 2007) {
        row.add(String.format("%d-%d-%d", myear + remodel, mmonth + remodel, mday));
      }
      else {
        row.add(String.format("%s", nullflag));
      }

      row.add(String.format("%d", randInt(10, 50))); // number of employees
      row.add(String.format("%d", randInt(10000, 30000))); // annual shrinkage
      row.add(String.format("%d", randInt(5, 500))); // foot traffic
      row.add(String.format("%d", randInt(100, 1000) + sqfoot)); // monthly rent cost

      rows.add(row.toArray(new String[row.size()]));
    }
    Batch batch = new Batch();
    batch.tableName = tableName;
    batch.rows = rows;

    queue.add(batch);
  }

  void promo_gen(String tableName, int numpromos)
  {
    String[] season = new String[] {
      "Summer", "Winter", "July 4th", "Thanksgiving", "Christmas"
    };
    String[] sale = new String[] {
      "Super", "Discount", "Liquidation", "Cool", "Mega"
    };
    String[] sale2 = new String[] {
      "Sale", "Sellathon", "Promotion"
    };
    String[] reduction = new String[] {
      "Half Price", "3 for price of 2", "50 Cents off", "Two for one", "20-50 percent off"
    };

    String[] media = new String[] {
      "Newspaper", "Magazine", "Radio", "TV", "Billboard", "Coupon", "Online"
    };

    String[] adtype = new String[] {
      "Fullpage", "Halfpage", "1 minute", "30 seconds"
    };

    String[] display = new String[] {
      "Kiosk", "End Aisle", "Shelf", "POS"
    };
    String[] coupon = new String[] {
      "Post", "Email", "Register Receipt", "Pennysaver", "Phone book"
    };
    String[] admedia = new String[] {
      "Time Magazine", "Superbowl Spot", "Boston Globe", "Other"
    };
    String[] displayprovider = new String[] {
      "Corporate", "Manufacturer", "Wholesaler", "Other"
    };


    // printf("Promotion_Dimension(promo_key|name|reduction|media|adtype|display|coupon|admedia|dispprov|cost|begin_date|end_date)\n");

    ArrayList<String[]> rows = Lists.newArrayList();

    for (int i = 1; i <= numpromos; i++) {
      List<String> row = Lists.newArrayList();

      row.add(String.format("%d", i));
      int s = randInt(0, 4);
      row.add(String.format("%s %s %s", season[s], sale[randInt(0, 4)], sale2[randInt(0, 2)]));
      row.add(String.format("%s", reduction[randInt(0, 4)]));
      row.add(String.format("%s", media[randInt(0, 6)]));
      row.add(String.format("%s", adtype[randInt(0, 3)]));
      row.add(String.format("%s", display[randInt(0, 3)]));
      row.add(String.format("%s", coupon[randInt(0, 4)]));
      row.add(String.format("%s", admedia[randInt(0, 3)]));
      row.add(String.format("%s", displayprovider[randInt(0, 3)]));
      row.add(String.format("%d", randInt(100, 500)));

      //summer, winter, july 4th, thanksgiving, christmas
      int month;
      if (s == 0) {
        month = randInt(5, 8);
      }
      else if (s == 1) {
        int y = randInt(0, 1);
        if (y != 0) {
          month = randInt(11, 12);
        }
        else {
          month = randInt(1, 2);
        }
      }
      else if (s == 2) {
        month = 7;
      }
      else if (s == 3) {
        month = 11;
      }
      else {
        month = 12; //s == 4
      }
      int day = randInt(1, 14);
      int year = randInt(2003, 2007);

      row.add(String.format("%d-%d-%d", year, month, day));
      row.add(String.format("%d-%d-%d", year, month, day + randInt(1, 14)));

      rows.add(row.toArray(new String[row.size()]));
    }
    Batch batch = new Batch();
    batch.tableName = tableName;
    batch.rows = rows;

    queue.add(batch);

  }

  void vend_gen(String tableName, int numvends)
  {
    String[] vendor = new String[] {
      "Food",
      "Market",
      "Deal",
      "Frozen",
      "Delicious",
      "Ripe",
      "Big Al's",
      "Sundry",
      "Everything"
    };
    String[] vendor2 = new String[] {
      "Wholesale",
      "Market",
      "Suppliers",
      "Mart",
      "Warehouse",
      "Farm",
      "Depot",
      "Outlet",
      "Discounters",
      "Emporium"
    };

    ArrayList<String[]> rows = Lists.newArrayList();
    for (int i = 1; i <= numvends; i++) {
      List<String> row = Lists.newArrayList();
      int loc = randInt(0, NUM_LOC - 1);
      row.add(String.format("%d", i)); //vendor key
      row.add(String.format("%s %s", vendor[randInt(0, 8)], vendor2[randInt(0, 9)])); //vendor name
      row.add(String.format("%d %s", randInt(1, 500), streets[randInt(0, 19)])); //street address
      row.add(String.format("%s", locations[loc][0]));
      row.add(String.format("%s", locations[loc][1]));
      row.add(String.format("%s", locations[loc][2]));
      row.add(String.format("%d", randInt(500, 1000000))); //deal size
      row.add(String.format("%d-%d-%d", randInt(2003, 2007), randInt(1, 12), randInt(1, 28))); // last deal update

      rows.add(row.toArray(new String[row.size()]));
    }
    Batch batch = new Batch();
    batch.tableName = tableName;
    batch.rows = rows;

    queue.add(batch);
  }

  void cust_gen(String tableName, int numcusts)
  {
    String[] marital = new String[] {
      "Single",
      "Engaged",
      "Married",
      "Divorced",
      "Separated",
      "Widowed",
      "Unknown"
    };
    String[] company = new String[] {
      "Ameri",
      "Veri",
      "Ini",
      "Virta",
      "Better",
      "Ever",
      "Info",
      "Gold",
      "Food",
      "Meta",
      "Intra"
    };
    String[] company2 = new String[] {
      "core",
      "care",
      "corp",
      "tech",
      "hope",
      "com",
      "shop",
      "gen",
      "data",
      "media",
      "star"
    };
    String[] occupation = new String[] {
      "Retired", "Farmer", "Mechanic", "Accountant", "Software Developer",
      "Student", "Painter", "Chemist", "Stock Broker", "Psychologist",
      "Writer", "Acrobat", "Teacher", "Banker", "Chef",
      "Hairdresser", "Waiter", "Police Officer", "Priest", "Rabbi",
      "Dancer", "Unemployed", "Doctor", "Exterminator", "Jeweler",
      "Detective", "Blacksmith", "Cobbler", "Fisherman", "Lumberjack",
      "Butler", "Meteorologist", "Musician", "Actor", "Other"
    };
    String[] stage = new String[] {
      "Prospect",
      "Negotiations",
      "Presented Solutions",
      "Collecting Requirements",
      "Closed - Lost",
      "Closed - Won"
    };

    ArrayList<String[]> rows = Lists.newArrayList();

    for (int i = 1; i <= numcusts; i++) {
      List<String> row = Lists.newArrayList();

      row.add(String.format("%d", i)); //customer key
      int loc = randInt(0, NUM_LOC - 1);
      int t = randInt(1, 30);

      if (t > 1) // customer is a single person
      {
        row.add(String.format("%s", "Individual"));
        int gend = randInt(0, 1);
        if (gend != 0) {
          row.add(String.format("%s %s. %s", mname[randInt(0, 29)],
                                midinit[randInt(0, 26)],
                                surname[randInt(0, 49)]));
          row.add(String.format("%s", "Male"));
          row.add(String.format("%s", title[randInt(0, 2)]));
        }
        else {
          row.add(String.format("%s %s. %s", fname[randInt(0, 29)],
                                midinit[randInt(0, 26)],
                                surname[randInt(0, 49)]));
          row.add(String.format("%s", "Female"));
          row.add(String.format("%s", title[randInt(2, 4)]));
        }
        row.add(String.format("%d", randInt(1, 30000))); // household id

        row.add(String.format("%d %s", randInt(1, 500), streets[randInt(0, 19)]));
        row.add(String.format("%s", locations[loc][0]));
        row.add(String.format("%s", locations[loc][1]));
        row.add(String.format("%s", locations[loc][2]));
        row.add(String.format("%s", marital[randInt(0, 6)]));
        row.add(String.format("%d", randInt(18, 70))); // age
        row.add(String.format("%d", randInt(0, 5))); // number of children
        row.add(String.format("%d", randInt(10000, 1000000))); // annual income
        row.add(String.format("%s", occupation[randInt(0, 33)]));
        row.add(String.format("%d", randInt(20, 1000))); // maximum bill amount
        row.add(String.format("%d", randInt(0, 1))); // store membership card
        // customer since
        row.add(String.format("%d-%d-%d", randInt(1965, 2007), randInt(1, 12), randInt(1, 28)));
        row.add(String.format("%s", nullflag));
        row.add(String.format("%s", nullflag));
        row.add(String.format("%s", nullflag));
      }
      else // customer is a company
      {
        row.add(String.format("%s", "Company"));
        row.add(String.format("%s%s", company[randInt(0, 10)], // company name
                              company2[randInt(0, 10)]));

        // company has no title, gender, or household
        row.add(String.format("%s", nullflag));
        row.add(String.format("%s", nullflag));
        row.add(String.format("%s", nullflag));

        int age = randInt(1, 100);
        row.add(String.format("%d %s", randInt(1, 500), streets[randInt(0, 19)]));
        row.add(String.format("%s", locations[loc][0]));
        row.add(String.format("%s", locations[loc][1]));
        row.add(String.format("%s", locations[loc][2]));
        //company has no marital status
        row.add(String.format("%s", nullflag));
        row.add(String.format("%d", age));
        //company has no # of children
        row.add(String.format("%s", nullflag));
        row.add(String.format("%d", randInt(500000, 100000000))); // company annual income
        //company has no occupation
        row.add(String.format("%s", nullflag));
        row.add(String.format("%d", randInt(1000, 500000))); // maximum bill amount
        row.add(String.format("%d", randInt(0, 1))); // store membership card

        row.add(String.format("%d-%d-%d", randInt(2007 - age, 2007),
                              randInt(1, 12), randInt(1, 28) // customer since
                ));
        row.add(String.format("%s", stage[randInt(0, 5)])); // deal stage
        row.add(String.format("%d", randInt(10000, 5000000))); // deal size
        row.add(String.format("%d-%d-%d", randInt(2003, 2007), randInt(1, 12) // last deal update
                , randInt(1, 28)));
      }

      rows.add(row.toArray(new String[row.size()]));
    }
    Batch batch = new Batch();
    batch.tableName = tableName;
    batch.rows = rows;

    queue.add(batch);
  }

  void emp_gen(String tableName, int numemps)
  {

    String[] topjob = new String[] {
      "CEO",
      "CFO",
      "Founder",
      "Co-Founder",
      "President",
      "Investor"
    };
    String[] highjob = new String[] {
      "VP of Sales",
      "VP of Advertising",
      "Head of Marketing",
      "Regional Manager",
      "Director of HR",
      "Head of PR"
    };
    String[] middlejob = new String[] {
      "Shift Manager",
      "Assistant Director",
      "Marketing",
      "Sales",
      "Advertising",
      "Branch Manager"
    };
    String[] lowjob = new String[] {
      "Shelf Stocker",
      "Delivery Person",
      "Cashier",
      "Greeter",
      "Customer Service",
      "Custodian"
    };

    ArrayList<String[]> rows = Lists.newArrayList();
    for (int i = 1; i <= numemps; i++) {
      List<String> row = Lists.newArrayList();
      int gend = randInt(0, 1);
      row.add(String.format("%d", i));
      if (gend != 0) {
        row.add(String.format("%s", "Male"));
        row.add(String.format("%s", title[randInt(0, 2)]));
        row.add(String.format("%s", mname[randInt(0, 29)]));
      }
      else {
        row.add(String.format("%s", "Female"));
        row.add(String.format("%s", title[randInt(2, 4)]));
        row.add(String.format("%s", fname[randInt(0, 29)]));
      }

      int loc = randInt(0, NUM_LOC - 1);
      int age = randInt(14, 65);
      row.add(String.format("%s", midinit[randInt(0, 26)]));
      row.add(String.format("%s", surname[randInt(0, 49)]));
      row.add(String.format("%d", age));
      row.add(String.format("%d-%d-%d", randInt(2007 - age + 14, 2007), randInt(1, 12), randInt(1, 28))); // hire date
      row.add(String.format("%d %s", randInt(1, 500), streets[randInt(0, 19)]));
      row.add(String.format("%s", locations[loc][0]));
      row.add(String.format("%s", locations[loc][1]));
      row.add(String.format("%s", locations[loc][2]));

      if (i <= numemps / 100) // make a chain of command
      {
        row.add(String.format("%s", topjob[randInt(0, 5)]));
        // top employees don't report to anyone
        row.add(String.format("%s", nullflag));
        row.add("1");
        row.add(String.format("d", randInt(200000, 1000000)));

        row.add(String.format("%s", nullflag));
        row.add(String.format("%d", randInt(20, 40)));
      }
      else if (i <= numemps / 5) {
        row.add(String.format("%s", highjob[randInt(0, 5)]));
        row.add(String.format("%d", randInt(1, numemps / 100)));
        row.add("1");
        row.add(String.format("%d", randInt(100000, 200000)));

        row.add(String.format("%s", nullflag));
        row.add(String.format("%d", randInt(15, 30)));
      }
      else if (i <= numemps / 2) {
        row.add(String.format("%s", middlejob[randInt(0, 5)]));
        row.add(String.format("%d", randInt(numemps / 100, numemps / 5)));

        row.add("1");
        row.add(String.format("%d", randInt(50000, 100000)));
        row.add(String.format("%s", nullflag));
        row.add(String.format("%d", randInt(10, 20)));
      }
      else {
        int cents = randInt(0, 99);
        int dollars = randInt(6, 15);
        row.add(String.format("%s", lowjob[randInt(0, 5)]));
        row.add(String.format("%d", randInt(numemps / 5, numemps / 2)));
        row.add("0");
        row.add(String.format("%d", (dollars * 200) + (cents * 2))); // annual salary assuming 200 hrs/year
        row.add(String.format("%d.%02d", dollars, cents));
        row.add(String.format("%d", randInt(5, 15)));
      }

    }
    Batch batch = new Batch();
    batch.tableName = tableName;
    batch.rows = rows;

    queue.add(batch);
  }

  int time_gen(String tableName, String timeFile)
  {
    char[] date = new char[15];
    char[] desc = new char[100];
    String[] dayofweek = new String[] {
      "",
      "Sunday",
      "Monday",
      "Tuesday",
      "Wednesday",
      "Thursday",
      "Friday",
      "Saturday",};
    int weekDay;

    int dayMonth, dayYear = 0, dayFisMonth, dayFisYear;

    int lastWeekDay, lastMonthDay;

    int weekYear;
    char[] weekLastDate = new char[10];
    int leapYear;

    String[] monthName = new String[] {
      "",
      "January",
      "February",
      "March",
      "April",
      "May",
      "June",
      "July",
      "August",
      "September",
      "October",
      "November",
      "December"
    };
    Integer[] lastDayOfMonth = new Integer[] {
      0,
      31,
      -1,
      31,
      30,
      31,
      30,
      31,
      31,
      30,
      31,
      30,
      31
    };

    int monthYear;
    char[] yearMonth = new char[20];
    String[] quarter = new String[] {
      "",
      "Q1",
      "Q1",
      "Q1",
      "Q2",
      "Q2",
      "Q2",
      "Q3",
      "Q3",
      "Q3",
      "Q4",
      "Q4",
      "Q4"
    };
    char[] yearQuarter = new char[20];
    char[] halfYear = new char[10];
    int year;
    int isHoliday, isWeekday;
    String line;
    int size_t, linesize = 0;
    int date_key = 1;
    try {
      InputStream resourceAsStream = getClass().getResourceAsStream("/" + timeFile);
      BufferedReader br = new BufferedReader(new InputStreamReader(resourceAsStream));

      // printf("Date_Dimension(date-key,date,Full Date Description, DayOfWeek, DayNumInMonth, DayNumInYear, DayNumInFisMonth, DayNumInFisYear, LastDayOfWeek, LastDayOfMonth, WeekNumInYear, MonthName, MonthNumInYear, Year-Month, Quarter, Year-Quarter, HalfYear, Year, Holiday, Weekday, Season\n");

      ArrayList<String[]> rows = Lists.newArrayList();

      String tline;
      while ((tline = br.readLine()) != null) {
        if (tline.charAt(0) == '#') {
          continue;
        }

        List<String> row = Lists.newArrayList();

        String[] tvals = tline.split(" ");
        date = tvals[0].toCharArray();
        dayMonth = Integer.valueOf(tvals[1]);
        monthYear = Integer.valueOf(tvals[2]);
        year = Integer.valueOf(tvals[3]);
        weekDay = Integer.valueOf(tvals[4]);
        weekYear = Integer.valueOf(tvals[5]);
        leapYear = Integer.valueOf(tvals[6]);

        if (dayMonth == 1 && monthYear == 1) {
          dayYear = 1; // reset
        }
        desc = String.format("%s %d, %d", monthName[monthYear], dayMonth, year).toCharArray();

        row.add(String.format("%d", date_key));
        row.add(String.format("%10s", date));
        row.add(String.format("%s", desc));
        row.add(String.format("%s", dayofweek[weekDay]));
        row.add(String.format("%d", dayMonth));
        row.add(String.format("%d", dayYear));
        row.add(String.format("%d", dayMonth));
        row.add(String.format("%d", dayYear));   // fiscal
        row.add(String.format("%d", (weekDay == 7) ? 1 : 0)); // last day of week
        row.add(String.format("%d",
                              ((dayMonth == lastDayOfMonth[monthYear])
                || (monthYear == 2 /* Feb */ && ((leapYear == 0) && dayMonth == 28)
                || ((leapYear != 0) && dayMonth == 29))) ? 1 : 0));
        row.add(String.format("%d", weekYear));
        row.add(String.format("%s", monthName[monthYear]));
        row.add(String.format("%d", monthYear));
        row.add(String.format("%d-%d", year, monthYear));
        row.add(String.format("%d", (monthYear < 4 ? 1
                                     : monthYear < 7 ? 2
                                       : monthYear < 10 ? 3 : 4)));
        row.add(String.format("%d-%s", year, quarter[monthYear]));
        row.add(String.format("%d", monthYear <= 6 ? 1 : 2));
        row.add(String.format("%d", year));
        row.add(String.format("%s", ((monthYear == 1 && dayYear == 1)
                || (monthYear == 7 && dayMonth == 4)
                || (monthYear == 10 && dayMonth == 30)
                || (monthYear == 12 && dayMonth == 25)) ? "Holiday" : "NonHoliday"));
        row.add(String.format("%s", (weekDay == 1 || weekDay == 7) ? "Weekend" : "Weekday"));
        row.add(String.format("%s", ((monthYear < 2) ? "ValentinesDay"
                                     : (monthYear < 4) ? "Easter"
                                       : (monthYear < 8) ? "July4th"
                                         : (monthYear < 10) ? "Thanksgiving"
                                           : "Christmas")));

        rows.add(row.toArray(new String[row.size()]));
        dayYear++;        // increment day of year
        date_key++;
      }

      //Batch batch = new Batch();
      //batch.tableName = tableName;
      //batch.rows = rows;

      //queue.add(batch);
    }
    catch (IOException ex) {
      DTThrowable.rethrow(ex);
    }
    return date_key;
  }

  void warehouse_gen(String tableName, int numwarehouses)
  {
    String[] warehousename = new String[] {
      "Warehouse 1",
      "Warehouse 2",
      "Warehouse 3",
      "Warehouse 4",
      "Warehouse 5"
    };

    ArrayList<String[]> rows = Lists.newArrayList();
    for (int i = 1; i <= numwarehouses; i++) {
      List<String> row = Lists.newArrayList();
      int loc = randInt(0, NUM_LOC - 1);
      row.add(String.format("%d", i));
      row.add(String.format("%s", warehousename[randInt(0, 4)]));
      row.add(String.format("%d %s", randInt(1, 500), streets[randInt(0, 19)]));
      row.add(String.format("%s", locations[loc][0]));
      row.add(String.format("%s", locations[loc][1]));
      row.add(String.format("%s", locations[loc][2]));
    }

    Batch batch = new Batch();
    batch.tableName = tableName;
    batch.rows = rows;

    queue.add(batch);

  }

  void shipping_gen(String tableName, int numshipping)
  {

    String[] shiptype = new String[] {
      "REGULAR",
      "EXPRESS",
      "NEXT DAY",
      "OVERNIGHT",
      "TWO DAY",
      "LIBRARY"
    };

    String[] shipmode = new String[] {
      "AIR",
      "SURFACE",
      "SEA",
      "BIKE",
      "HAND CARRY",
      "MESSENGER",
      "COURIER"
    };

    String[] shipcarrier = new String[] {
      "UPS",
      "FEDEX",
      "AIRBORNE",
      "USPS",
      "DHL",
      "TBS",
      "ZHOU",
      "ZOUROS",
      "MSC",
      "LATVIAN",
      "ALLIANCE",
      "ORIENTAL",
      "BARIAN",
      "BOXBUNDLES",
      "GREAT EASTERN",
      "DIAMOND",
      "RUPEKSA",
      "GERMA",
      "HARMSTORF",
      "PRIVATECARRIER"
    };

    ArrayList<String[]> rows = Lists.newArrayList();

    for (int i = 1; i <= numshipping; i++) {
      List<String> row = Lists.newArrayList();
      row.add(String.format("%d", i));
      row.add(String.format("%s", shiptype[randInt(0, 5)]));
      row.add(String.format("%s", shipmode[randInt(0, 6)]));
      row.add(String.format("%s", shipcarrier[randInt(0, 19)]));
    }
    Batch batch = new Batch();
    batch.tableName = tableName;
    batch.rows = rows;

    queue.add(batch);
  }

  void onlinepage_gen(String tableName, int numonlinepages)
  {
    String[] op_description = new String[] {
      "Online Page Description #1",
      "Online Page Description #2",
      "Online Page Description #3",
      "Online Page Description #4",
      "Online Page Description #5",
      "Online Page Description #6",
      "Online Page Description #7",
      "Online Page Description #8"
    };

    String[] op_type = new String[] {
      "bi-annual",
      "quarterly",
      "monthly"
    };

    ArrayList<String[]> rows = Lists.newArrayList();

    for (int i = 1; i <= numonlinepages; i++) {
      List<String> row = Lists.newArrayList();
      row.add(String.format("%d", i));

      int month = randInt(1, 12);
      int day = randInt(1, 14);
      int year = randInt(2003, 2007);

      row.add(String.format("%d-%d-%d", year, month, day));
      row.add(String.format("%d-%d-%d", year, month, day + randInt(1, 14)));

      row.add(String.format("%d", randInt(1, 40)));
      row.add(String.format("%s", op_description[randInt(0, 7)]));
      row.add(String.format("%s", op_type[randInt(0, 2)]));

    }
    Batch batch = new Batch();
    batch.tableName = tableName;
    batch.rows = rows;

    queue.add(batch);
  }

  void callcenter_gen(String tableName, int numcallcenters)
  {
    String[] cc_name = new String[] {
      "New England",
      "NY Metro",
      "Mid Atlantic",
      "Southeastern",
      "North Midwest",
      "Central Midwest",
      "South Midwest",
      "Pacific Northwest",
      "California",
      "Southwest",
      "Hawaii/Alaska",
      "Other"
    };

    String[] cc_class = new String[] {
      "small",
      "medium",
      "large"
    };

    String[] cc_hours = new String[] {
      "8AM-4PM",
      "8AM-12AM",
      "8AM-8AM"
    };

    String[] cc_manager = new String[] {
      "Bob Belcher",
      "Felipe Perkins",
      "Mark Hightower",
      "Larry Mccray"
    };

    int month, day, year;

    ArrayList<String[]> rows = Lists.newArrayList();
    for (int i = 1; i <= numcallcenters; i++) {
      List<String> row = Lists.newArrayList();

      int loc = randInt(0, NUM_LOC - 1);

      row.add(String.format("%d", i));

      month = randInt(1, 12);
      day = randInt(1, 14);
      year = randInt(2003, 2007);

      row.add(String.format("%d-%d-%d", year, month, day + randInt(1, 14)));
      row.add(String.format("%d-%d-%d", year, month, day));

      row.add(String.format("%s", cc_name[randInt(0, 11)]));
      row.add(String.format("%s", cc_class[randInt(0, 2)]));
      row.add(String.format("%d", randInt(1, 30)));
      row.add(String.format("%s", cc_hours[randInt(0, 2)]));
      row.add(String.format("%s", cc_manager[randInt(0, 3)]));
      row.add(String.format("%d %s", randInt(1, 500), streets[randInt(0, 19)]));
      row.add(String.format("%s", locations[loc][0]));
      row.add(String.format("%s", locations[loc][1]));
      row.add(String.format("%s", locations[loc][2]));

    }
    Batch batch = new Batch();
    batch.tableName = tableName;
    batch.rows = rows;

    queue.add(batch);
  }

  void salesfact_gen(String tableName, int numfactsalesrows)
  {
    // STORE_SALES_FACT
    int numsalesrowsperfile = numfactsalesrows / numfiles;
    // printf("POS_Retail_Sales_Transaction_Fact(date_key, product_key, store_key, promo_key, trans_id, quantity, dollar_amount, dollar_cost, profit, trans_type,  trans_time, tender_type)\n");
    int numsalesrows = numfactsalesrows;
    String[] tendertype = new String[] {
      "Cash",
      "Credit",
      "Debit",
      "Check",
      "Other"
    };

    ArrayList<String[]> rows = Lists.newArrayList();
    while (numsalesrows > 0) {
      for (int t = 1; (t <= numtimekeys) && numsalesrows > 0; t++) // on each day
      {
        int transleft = numsalesrows;
        int keysleft = numtimekeys - t;
        int tpd = transleft / (keysleft + 1);
        int numtrans = randInt(0, tpd * 2); // numprods product transactions
        if (keysleft == 0) {
          numtrans = numsalesrows;
        }

        int store = 0, promo = 0, cust = 0, emp = 0, hour = 0, minute = 0, second = 0, tend = 0;

        for (int i = 0; (i < numtrans) && numsalesrows > 0; i++) {
          // start new file after numsalesfactrows # of rows.
          // last file may get some extra

          if (((numfactsalesrows - numsalesrows) % numsalesrowsperfile == 0)) {
            int cost = randInt(1, 300);
            int profit = randInt(5, 300);
            int sametrans = randInt(0, 3);
            if (sametrans == 0 || i == 0 || (hour == 23 && minute == 59 && second > 29)) //only change the info sometimes so that
            {                            //a transaction can have the same customer
              //buying several different products
              store = randInt(1, numstorekeys);
              promo = randInt(1, numpromokeys);
              cust = randInt(1, numcustkeys);
              emp = randInt(1, numempkeys);

              hour = randInt(0, 23);
              minute = randInt(0, 59);
              second = randInt(0, 59);
              tend = randInt(0, 4);
            }
            else // same transaction, just increase the time by a small amount
            {
              int newsec = randInt(10, 30) + second;
              if (newsec >= 60) {
                if (minute == 59) {
                  minute = 0;
                  hour++; // hour is reset if we're nearing a new day, so no need
                }         // to check that hour isn't 23 before incrementing
                else {
                  minute++;
                }
              }
              second = newsec % 60;
            }

            String transtype;
            int tt = randInt(1, 20);
            if (tt > 1) {
              transtype = "purchase";
            }
            else {
              transtype = "return";
              profit *= -1;
              cost *= -1;
            }

            int prod = randInt(1, numprods);

            List<String> row = Lists.newArrayList();
            row.add(String.format("%d", t)); // timekey
            row.add(String.format("%d", prod)); // product is always different
            row.add(String.format("%d", randInt(1, prodVersionArray[prod])));
            row.add(String.format("%d", store));
            row.add(String.format("%d", promo));
            row.add(String.format("%d", cust));
            row.add(String.format("%d", emp));
            row.add(String.format("%d", numfactsalesrows - numsalesrows + 1)); // transaction number
            row.add(String.format("%d", randInt(1, 10))); // Quantity
            row.add(String.format("%d", cost + profit)); // sales dollar amount
            row.add(String.format("%d", cost)); // cost amount
            row.add(String.format("%d", profit)); // gross profit
            row.add(String.format("%s", transtype)); // transaction type

            row.add(String.format("%02d:%02d:%02d", hour, minute, second));
            row.add(String.format("%s", tendertype[tend]));
            rows.add(row.toArray(new String[row.size()]));

            numsalesrows--;
          }
        }
      }
    }
    Batch batch = new Batch();
    batch.tableName = tableName;
    batch.rows = rows;

    queue.add(batch);
  }

  void storeorder_gen(String tableName, int numcallcenters)
  {
    //STORE_ORDER_FACT
    // Now generate the sales_orders_fact table
    int numorderrowsperfile = numfactorderrows / numfiles;
    String[] shippers = new String[] {
      "American Delivery",
      "Speedy Go",
      "TransFast",
      "Postal Service",
      "Shipping Xperts",
      "FlyBy Shippers",
      "DHX",
      "UPL",
      "Package Throwers",
      "United Express"
    };
    ArrayList<String[]> rows = Lists.newArrayList();
    int numorderrows = numfactorderrows;
    while (numorderrows > 0) {
      if (((numfactorderrows - numorderrows) % numorderrowsperfile == 0)) {
        int prod = randInt(1, numprods);
        List<String> row = Lists.newArrayList();
        row.add(String.format("%d", prod));
        row.add(String.format("%d", randInt(1, prodVersionArray[prod])));
        row.add(String.format("%d", randInt(1, numstorekeys)));
        row.add(String.format("%d", randInt(1, numvendkeys)));
        row.add(String.format("%d", randInt(numempkeys / 100, numempkeys)));
        row.add(String.format("%d", numfactorderrows - numorderrows + 1)); // order number

        int omonth = randInt(1, 12);
        int oday = randInt(1, 10);
        int oyear = randInt(2003, 2007);
        int sday = oday + randInt(0, 5);
        int dday = sday + randInt(3, 10);

        row.add(String.format("%d-%d-%d", oyear, omonth, oday)); // date ordered
        row.add(String.format("%d-%d-%d", oyear, omonth, sday)); // date shipped
        row.add(String.format("%d-%d-%d", oyear, omonth, dday)); // date delivered
        row.add(String.format("%d-%d-%d", oyear, omonth, oday + 7)); // expected delivery date

        int quantity = randInt(5, 100);
        int lost = randInt(1, 40);
        if (lost > 4) {
          lost = 0;
        }

        row.add(String.format("%d", quantity));
        row.add(String.format("%d", quantity - lost));

        int price = randInt(1, 300);
        int shipping = randInt(10, 200);
        row.add(String.format("%s", shippers[randInt(0, 9)]));
        row.add(String.format("%d", price));
        row.add(String.format("%d", shipping));
        row.add(String.format("%d", price * quantity + shipping));
        row.add(String.format("%d", randInt(10, 100))); // quantity in stock
        row.add(String.format("%d", randInt(10, 50))); // reorder level
        row.add(String.format("%d", randInt(50, 150))); // overstock ceiling

        rows.add(row.toArray(new String[row.size()]));
        numorderrows--;
      }

    }
    Batch batch = new Batch();
    batch.tableName = tableName;
    batch.rows = rows;

    queue.add(batch);
  }

  void inventoryfact_gen(String tableName, int numcallcenters)
  {
    //INVENTORY_FACT
    int numinventoryrowsperfile = numfactinventoryrows / numfiles;
    int numinventoryrows = numfactinventoryrows;
    ArrayList<String[]> rows = Lists.newArrayList();
    while (numinventoryrows > 0) {
      for (int t = 1; (t <= numtimekeys) && numinventoryrows > 0; t++) // on each day
      {
        int transleft = numinventoryrows;
        int keysleft = numtimekeys - t;
        int tpd = transleft / (keysleft + 1);
        int numtrans = randInt(0, tpd * 2); // numprods product transactions
        if (keysleft == 0) {
          numtrans = numinventoryrows;
        }

        for (int i = 0; (i < numtrans) && numinventoryrows > 0; i++) {
          // start new file after numsalesfactrows # of rows.
          // last file may get some extra
          if (((numfactinventoryrows - numinventoryrows) % numinventoryrowsperfile == 0)) {

            List<String> row = Lists.newArrayList();
            int prod = randInt(1, numprods);
            row.add(String.format("%d", t)); // timekey
            row.add(String.format("%d", prod)); // product is always different
            row.add(String.format("%d", randInt(1, prodVersionArray[prod])));
            row.add(String.format("%d", randInt(1, numwarehousekeys)));
            row.add(String.format("%d", randInt(1, 1000)));

            rows.add(row.toArray(new String[row.size()]));
            numinventoryrows--;
          }
        }
      }
    }
    Batch batch = new Batch();
    batch.tableName = tableName;
    batch.rows = rows;

    queue.add(batch);
  }

  void onlinesalesfact_gen(String tableName, int numcallcenters)
  {
    //ONLINE_SALES_FACT
    int numonlinesalesrowsperfile = numfactonlinesalesrows / numfiles;
    int numonlinesalesrows = numfactonlinesalesrows;
    ArrayList<String[]> rows = Lists.newArrayList();
    while (numonlinesalesrows > 0) {
      for (int t = 1; (t <= numtimekeys) && numonlinesalesrows > 0; t++) // on each day
      {
        int transleft = numonlinesalesrows;
        int keysleft = numtimekeys - t;
        int tpd = transleft / (keysleft + 1);
        int numtrans = randInt(0, tpd * 2); // numprods product transactions
        if (keysleft == 0) {
          numtrans = numonlinesalesrows;
        }

        for (int i = 0; (i < numtrans) && numonlinesalesrows > 0; i++) {
          // start new file after numsalesfactrows # of rows.
          // last file may get some extra
          if (((numfactonlinesalesrows - numonlinesalesrows) % numonlinesalesrowsperfile == 0)) {
            int cost = randInt(1, 300);
            int profit = randInt(5, 300);
            int ship_amount = randInt(5, 15);

            String transtype;
            int tt = randInt(1, 20);
            if (tt > 1) {
              transtype = "purchase";
            }
            else {
              transtype = "return";
              profit *= -1;
              cost *= -1;
              ship_amount *= -1;
            }

            int prod = randInt(1, numprods);

            int shipdatekey;
            if (keysleft <= 5) {
              shipdatekey = t;
            }
            else {
              shipdatekey = t + randInt(1, 5);
            }

            List<String> row = Lists.newArrayList();
            row.add(String.format("%d", t)); // timekey
            row.add(String.format("%d", shipdatekey)); //ship_date_key
            row.add(String.format("%d", prod)); // product is always different
            row.add(String.format("%d", randInt(1, prodVersionArray[prod])));
            row.add(String.format("%d", randInt(1, numcustkeys)));
            row.add(String.format("%d", randInt(1, numcallcenterkeys)));
            row.add(String.format("%d", randInt(1, numonlinepagekeys)));
            row.add(String.format("%d", randInt(1, numshippingkeys)));
            row.add(String.format("%d", randInt(1, numwarehousekeys)));
            row.add(String.format("%d", randInt(1, numpromokeys)));
            row.add(String.format("%d", numfactonlinesalesrows - numonlinesalesrows + 1)); // transaction number
            row.add(String.format("%d", randInt(1, 10))); // Quantity
            row.add(String.format("%d", cost + profit)); // sales dollar amount
            row.add(String.format("%d", ship_amount)); //ship dollar amount
            row.add(String.format("%d", cost + profit + ship_amount)); // net dollar amount
            row.add(String.format("%d", cost)); // cost dollar amount
            row.add(String.format("%d", profit)); // gross profit dollar amount
            row.add(String.format("%s", transtype));

            rows.add(row.toArray(new String[row.size()]));
            numonlinesalesrows--;
          }
        }
      }

    }
    Batch batch = new Batch();
    batch.tableName = tableName;
    batch.rows = rows;

    queue.add(batch);
  }

  public static int randInt(int min, int max)
  {
    Random rand = new Random();
    int randomNum = rand.nextInt((max - min) + 1) + min;

    return randomNum;
  }

  /*
   typedef struct
   {
   const char *city;
   const char *state;
   const char *region;
   } Location;
   */

  public int getNumprods()
  {
    return numprods;
  }

  public void setNumprods(int numprods)
  {
    this.numprods = numprods;
  }

  public int getNumprodkeys()
  {
    return numprodkeys;
  }

  public void setNumprodkeys(int numprodkeys)
  {
    this.numprodkeys = numprodkeys;
  }

  public int getNumstorekeys()
  {
    return numstorekeys;
  }

  public void setNumstorekeys(int numstorekeys)
  {
    this.numstorekeys = numstorekeys;
  }

  public int getNumpromokeys()
  {
    return numpromokeys;
  }

  public void setNumpromokeys(int numpromokeys)
  {
    this.numpromokeys = numpromokeys;
  }

  public int getNumvendkeys()
  {
    return numvendkeys;
  }

  public void setNumvendkeys(int numvendkeys)
  {
    this.numvendkeys = numvendkeys;
  }

  public int getNumcustkeys()
  {
    return numcustkeys;
  }

  public void setNumcustkeys(int numcustkeys)
  {
    this.numcustkeys = numcustkeys;
  }

  public int getNumempkeys()
  {
    return numempkeys;
  }

  public void setNumempkeys(int numempkeys)
  {
    this.numempkeys = numempkeys;
  }

  public int getNumfactsalesrows()
  {
    return numfactsalesrows;
  }

  public void setNumfactsalesrows(int numfactsalesrows)
  {
    this.numfactsalesrows = numfactsalesrows;
  }

  public int getNumfactorderrows()
  {
    return numfactorderrows;
  }

  public void setNumfactorderrows(int numfactorderrows)
  {
    this.numfactorderrows = numfactorderrows;
  }

  public int getNumfiles()
  {
    return numfiles;
  }

  public void setNumfiles(int numfiles)
  {
    this.numfiles = numfiles;
  }

  public String getNullflag()
  {
    return nullflag;
  }

  public void setNullflag(String nullflag)
  {
    this.nullflag = nullflag;
  }

  public int getNumwarehousekeys()
  {
    return numwarehousekeys;
  }

  public void setNumwarehousekeys(int numwarehousekeys)
  {
    this.numwarehousekeys = numwarehousekeys;
  }

  public int getNumshippingkeys()
  {
    return numshippingkeys;
  }

  public void setNumshippingkeys(int numshippingkeys)
  {
    this.numshippingkeys = numshippingkeys;
  }

  public int getNumonlinepagekeys()
  {
    return numonlinepagekeys;
  }

  public void setNumonlinepagekeys(int numonlinepagekeys)
  {
    this.numonlinepagekeys = numonlinepagekeys;
  }

  public int getNumcallcenterkeys()
  {
    return numcallcenterkeys;
  }

  public void setNumcallcenterkeys(int numcallcenterkeys)
  {
    this.numcallcenterkeys = numcallcenterkeys;
  }

  public int getNumfactonlinesalesrows()
  {
    return numfactonlinesalesrows;
  }

  public void setNumfactonlinesalesrows(int numfactonlinesalesrows)
  {
    this.numfactonlinesalesrows = numfactonlinesalesrows;
  }

  public int getNumfactinventoryrows()
  {
    return numfactinventoryrows;
  }

  public void setNumfactinventoryrows(int numfactinventoryrows)
  {
    this.numfactinventoryrows = numfactinventoryrows;
  }

  public int getNumtimekeys()
  {
    return numtimekeys;
  }

  public void setNumtimekeys(int numtimekeys)
  {
    this.numtimekeys = numtimekeys;
  }

  public boolean isGenerate()
  {
    return generate;
  }

  public void setGenerate(boolean generate)
  {
    this.generate = generate;
  }
}
