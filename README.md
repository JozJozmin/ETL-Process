# ETL-Process
Building an  ETL Process with Clickstream Data in PySpark for a global online e-commerce company GoShop.<br>
**Problem Statement:** <br>
GoShop is an online e-commerce company that caters to different products to users across the globe. The company is growing at an exponential
rate and has seen tremendous growth in the last 2 years.
The users can place their orders through the company website. The company tracks each and every activity of the user on the website. This
information collected from the user activity is known as Clickstream data.
Clickstream data is the information that is collected about a user while they are browsing through a website. This includes a wide range of
information such as <br>
          <li> Which browser did the user use to visit the website?
          <li> What page did the user visit on the website?
          <li> Was the user logged in while visiting the website?
          <li> Did the user click on a certain element of the webpage? <br>
Now, the company would like to use Clickstream data to understand the user behavior on the website.In this project build an ETL process with the Clickstream data.

**Objective:** <br>
Implement an ETL process and generate a table that will help the company to know more about its user base. This table can be used by the company in answering the following questions
            <li> The users who visited on a particular date.
            <li> How many registered and non-registered users visited the website on a particular date?
            <li> Which was the first URL the user visited on the website on a particular date?
            <li> Determining the no. of Clickstream events occurring on the website.<br>
**About the Dataset** <br>
dataset containing 2 files - Clickstream (JSON format) and Login (CSV format).<br>
**Clickstream Dataset Informations:** <br>
              <li> browser_id (string) - Id of browser from which user is accessing the website.
              <li> session_id (string) - Id of the session created for the visiting user
              <li> client_side_data (string) - Embedded JSON element containing - current_page_url, time_elapsed <br>
                                 <li>   current_page_url - The URL of the page the user has visited with the click.
                                 <li>   time_elapsed - The time spent by the user on the particular page.
              <li> event_date_time (string) - Date and time when the event occurred on the website.
              <li> event_type (string) - Whether the click was a simple click event or a pageload event. A new page is loaded on a pageload event.<br>
**Login Dataset Informations:** <br>
              <li> login_date_time (string) - Date and time at which the user logged in to the website.
              <li> session_id (string) - Id of the session created for the visiting user
              <li> user_id (string) - Unique id of registered user on the website <br>

**Result:** <br>
 Private Leaderboard : 33 Public Leaderboard : 33 <br>
 Score: 0.9004591545
         
               
