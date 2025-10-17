library(httr)
library(jsonlite)
library(dplyr)
library(purrr)
library(shiny)
library(MASS)
library(lubridate)

# Helper for safe default
`%||%` <- function(a, b) if (!is.null(a)) a else b

# ======================================================================
# GLOBAL DATA: NHL Teams for Dropdown
# ======================================================================
NHL_TEAMS <- c(
  "Anaheim Ducks", "Boston Bruins", "Buffalo Sabres",
  "Calgary Flames", "Carolina Hurricanes", "Chicago Blackhawks",
  "Colorado Avalanche", "Columbus Blue Jackets", "Dallas Stars",
  "Detroit Red Wings", "Edmonton Oilers", "Florida Panthers",
  "Los Angeles Kings", "Minnesota Wild", "Montreal Canadiens",
  "Nashville Predators", "New Jersey Devils", "New York Islanders",
  "New York Rangers", "Ottawa Senators", "Philadelphia Flyers",
  "Pittsburgh Penguins", "San Jose Sharks", "Seattle Kraken",
  "St. Louis Blues", "Tampa Bay Lightning", "Toronto Maple Leafs",
  "Utah Mammoth", "Vancouver Canucks", "Vegas Golden Knights",
  "Washington Capitals", "Winnipeg Jets"
)
NHL_TEAMS <- sort(NHL_TEAMS)


# ======================================================================
# PART 1: DATA SCRAPING & HELPER FUNCTIONS
# ======================================================================

get_espn_scoreboard <- function(date) {
  url <- paste0("https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard?dates=", date)
  res <- httr::GET(url)
  stop_for_status(res)
  jsonlite::fromJSON(content(res, "text", encoding = "UTF-8"), flatten = FALSE)
}

extract_games_from_json <- function(js) {
  events <- js$events
  
  if (is.null(events) || !is.data.frame(events) || nrow(events) == 0) return(NULL)
  
  events_list <- purrr::transpose(events)
  
  map_dfr(events_list, function(game) {
    if (is.null(game$competitions) || length(game$competitions) == 0) return(NULL)
    comp <- game$competitions[1, ]
    if (is.null(comp$competitors) || length(comp$competitors) == 0) return(NULL)
    competitors <- comp$competitors[[1]]
    home <- dplyr::filter(competitors, homeAway == "home")
    away <- dplyr::filter(competitors, homeAway == "away")
    if (nrow(home) != 1 || nrow(away) != 1) return(NULL)
    
    home_team_name <- home$team$displayName %||% NA
    away_team_name <- away$team$displayName %||% NA
    home_score <- suppressWarnings(as.numeric(home$score %||% NA))
    away_score <- suppressWarnings(as.numeric(away$score %||% NA))
    
    data.frame(
      game_id = game$id %||% NA,
      # Convert date to R Date format for filtering
      date = as.Date(substr(game$date %||% NA, 1, 10)), 
      home_team = home_team_name,
      away_team = away_team_name,
      home_goals = home_score,
      away_goals = away_score,
      stringsAsFactors = FALSE
    )
  })
}

get_nhl_data_for_dates <- function(dates) {
  if (!is.vector(dates) || !is.character(dates)) {
    stop("Input 'dates' must be a vector of dates in 'YYYYMMDD' character format.")
  }
  
  all_games_list <- purrr::map(dates, function(date) {
    message(paste("Fetching data for:", date)) 
    js <- tryCatch(
      get_espn_scoreboard(date),
      error = function(e) {
        suppressWarnings(message(paste("Error fetching data for", date, ":", conditionMessage(e))))
        return(NULL)
      }
    )
    if (is.null(js)) return(NULL)
    games_df <- tryCatch(
      extract_games_from_json(js),
      error = function(e) {
        suppressWarnings(message(paste("Error extracting games for", date, ":", conditionMessage(e))))
        return(NULL)
      }
    )
    return(games_df)
  })
  
  return(bind_rows(all_games_list))
}

generate_date_range <- function(start_date, end_date) {
  dates <- seq(
    as.Date(start_date, format = "%Y%m%d"),
    as.Date(end_date, format = "%Y%m%d"),
    by = "day"
  )
  format(dates, "%Y%m%d")
}

# ======================================================================
# *** CACHING SECTION: SCRAPES 30 DAYS ONCE AT APP STARTUP ***
# ======================================================================

# Define the maximum window for the cache
CACHING_DAYS <- 30
END_DATE <- Sys.Date()
START_DATE <- END_DATE - CACHING_DAYS
CACHED_DATES <- generate_date_range(format(START_DATE, "%Y%m%d"), format(END_DATE, "%Y%m%d"))

# Fetch the data once globally and cache it
cat(paste("CACHING: Scraping", CACHING_DAYS, "days of historical data from", START_DATE, "to", END_DATE, "\n"))
CACHED_HISTORICAL_DATA <- get_nhl_data_for_dates(CACHED_DATES)

if (is.null(CACHED_HISTORICAL_DATA) || nrow(CACHED_HISTORICAL_DATA) == 0) {
  cat("WARNING: Caching failed. CACHED_HISTORICAL_DATA is empty.\n")
  CACHED_HISTORICAL_DATA <- data.frame(date = as.Date(character())) # Empty DF with date column
} else {
  cat(paste("CACHING: Successfully cached", nrow(CACHED_HISTORICAL_DATA), "games.\n"))
}

# Add a CACHED_UNTIL variable for the note in the UI
CACHED_UNTIL <- format(END_DATE, "%B %d, %Y")

# ======================================================================
# PART 2: NEGATIVE BINOMIAL MODEL FUNCTIONS
# ======================================================================

build_count_model <- function(df) {
  if (is.null(df) || nrow(df) == 0) {
    stop("Input data frame is empty or NULL. Cannot train model.")
  }
  
  home_data <- df %>% dplyr::mutate(is_home = TRUE) %>%
    dplyr::select(game_id, goals_scored = home_goals, goals_conceded = away_goals,
                  team = home_team, opponent = away_team, is_home)
  away_data <- df %>% dplyr::mutate(is_home = FALSE) %>%
    dplyr::select(game_id, goals_scored = away_goals, goals_conceded = home_goals,
                  team = away_team, opponent = home_team, is_home)
  
  stacked_df <- bind_rows(home_data, away_data) %>%
    dplyr::filter(!is.na(goals_scored) & !is.na(goals_conceded))
  
  if (nrow(stacked_df) == 0) {
    stop("No completed games found to train the model.")
  }
  
  count_model <- MASS::glm.nb(
    goals_scored ~ team + opponent + is_home,
    data = stacked_df
  )
  
  return(count_model)
}

predict_expected_goals <- function(model, home_team, away_team) {
  predict_df <- data.frame(
    team = c(home_team, away_team),
    opponent = c(away_team, home_team),
    is_home = c(TRUE, FALSE)
  )
  lambda <- tryCatch(
    predict(model, predict_df, type = "response"),
    error = function(e) {
      stop(paste("Prediction failed. Ensure both teams (", home_team, " and ", away_team, ") played in the historical window."))
    }
  )
  
  return(list(
    home_lambda = lambda[predict_df$is_home == TRUE],
    away_lambda = lambda[predict_df$is_home == FALSE]
  ))
}

calculate_over_under_prob <- function(home_lambda, away_lambda, vegas_total) {
  MAX_GOALS <- 15
  goals <- 0:MAX_GOALS
  
  prob_matrix <- outer(dpois(goals, home_lambda), dpois(goals, away_lambda))
  total_goals_matrix <- outer(goals, goals, FUN = `+`)
  
  prob_under <- sum(prob_matrix[total_goals_matrix < vegas_total])
  prob_over <- sum(prob_matrix[total_goals_matrix > vegas_total])
  prob_push <- sum(prob_matrix[total_goals_matrix == vegas_total])
  
  total_prob <- prob_under + prob_over + prob_push
  
  return(list(
    prob_under = prob_under / total_prob,
    prob_over = prob_over / total_prob,
    prob_push = prob_push / total_prob,
    expected_total_goals = home_lambda + away_lambda
  ))
}

# ======================================================================
# PART 3: MASTER WRAPPER FUNCTION - USES SUBSET OF CACHED DATA
# ======================================================================

# Function now accepts the pre-filtered data
run_ou_prediction <- function(home_team, away_team, vegas_total, filtered_data) {
  
  if (is.null(filtered_data) || nrow(filtered_data) < 10) {
    stop(paste("Insufficient data (scraped:", nrow(filtered_data), "games) to train a reliable model. Select a different historical window."))
  }
  
  # 3. Train Negative Binomial Model
  message("Training Negative Binomial model...")
  nhl_model <- build_count_model(filtered_data) 
  
  # 4. Predict Expected Goals
  lambdas <- predict_expected_goals(nhl_model, home_team, away_team)
  
  # 5. Calculate O/U Probabilities
  probabilities <- calculate_over_under_prob(
    lambdas$home_lambda,
    lambdas$away_lambda,
    vegas_total = vegas_total
  )
  
  # 6. Format Output
  results_df <- data.frame(
    Metric = c("Matchup", "Vegas Line", "Model Expected Total", "Probability OVER", "Probability UNDER", "Probability PUSH"),
    Value = c(
      paste(away_team, "at", home_team),
      vegas_total,
      round(probabilities$expected_total_goals, 3),
      round(probabilities$prob_over, 4),
      round(probabilities$prob_under, 4),
      round(probabilities$prob_push, 4)
    ),
    stringsAsFactors = FALSE
  )
  
  return(results_df)
}

# ======================================================================
# PART 4: SHINY APPLICATION UI AND SERVER
# ======================================================================

# Define the User Interface (UI)
ui <- fluidPage(
  
  # Application title - UPDATED: Removed parenthetical
  titlePanel("🏒 NHL Over/Under Prediction Model"), 
  
  # Sidebar layout for user input
  sidebarLayout(
    sidebarPanel(
      
      # Input 1: Away Team (First)
      selectInput("away_team", "Away Team:",
                  choices = NHL_TEAMS,
                  selected = "New Jersey Devils"),
      
      # Input 2: Home Team (Second)
      selectInput("home_team", "Home Team:",
                  choices = NHL_TEAMS,
                  selected = "New York Rangers"),
      
      # Input 3: Vegas Total Line
      numericInput("vegas_total", "Vegas Over/Under Line:", value = 5.5, min = 1, max = 15, step = 0.5),
      
      # Input 4: History Days
      radioButtons("history_days", "Historical Data Window:",
                   choices = c("Last 15 Days" = 15, "Last 30 Days" = 30),
                   selected = 30),
      
      # Action button to run the model
      actionButton("run_model", "Run Prediction", class = "btn-success"),
      
      hr(),
      # ADDED: Note about Negative Binomial Regression
      p(tags$b("Technical Note:"), "This prediction is based on a negative binomial regression model."),
      
      hr(),
      # Data Note
      p(paste("Historical game results are scraped once at app startup (up to", CACHING_DAYS, "days) and are current as of", CACHED_UNTIL, ".")), 
      
      # Disclaimer
      hr(),
      tags$i(
        HTML("<b>Disclaimer:</b> This model is for informational and entertainment purposes only. It is not financial advice or a guarantee of outcome. Do not use this information to bet.")
      )
    ),
    
    # Main panel for output
    mainPanel(
      h3("Prediction Status"),
      verbatimTextOutput("status_message"),
      hr(),
      h3("Predicted Over/Under Probabilities"),
      tableOutput("prediction_table")
    )
  )
)

# Define the Server Logic
server <- function(input, output, session) {
  
  # Ensure home and away teams are different (Logic remains the same)
  observeEvent(input$home_team, {
    if (input$home_team == input$away_team) {
      other_teams <- NHL_TEAMS[NHL_TEAMS != input$home_team]
      new_away_team <- other_teams[1]
      updateSelectInput(session, "away_team", selected = new_away_team)
    }
  })
  
  observeEvent(input$away_team, {
    if (input$away_team == input$home_team) {
      other_teams <- NHL_TEAMS[NHL_TEAMS != input$away_team]
      new_home_team <- other_teams[1]
      updateSelectInput(session, "home_team", selected = new_home_team)
    }
  })
  
  
  # Reactive value to store the model results
  model_results <- eventReactive(input$run_model, {
    
    # Validation checks
    req(input$home_team, input$away_team, input$vegas_total)
    
    # Convert days input and define the required start date
    days_to_use <- as.numeric(input$history_days)
    filter_date <- Sys.Date() - days_to_use
    
    # 1. Filter the globally CACHED_HISTORICAL_DATA based on user input
    filtered_data <- CACHED_HISTORICAL_DATA %>%
      filter(date >= filter_date)
    
    # Final check before running
    if (input$home_team == input$away_team) {
      return("ERROR: Home and Away teams must be different to run the prediction.")
    }
    
    withProgress(message = 'Calculating...', detail = 'Filtering data and training model.', value = 0.5, { 
      
      # Call the master function, passing the filtered data subset
      result <- tryCatch({
        run_ou_prediction(
          home_team = input$home_team,
          away_team = input$away_team,
          vegas_total = input$vegas_total,
          filtered_data = filtered_data
        )
      }, error = function(e) {
        # Return error message on failure
        return(as.character(e))
      })
      
      incProgress(1.0)
      return(result)
    })
  })
  
  # Output for status/error messages
  output$status_message <- renderPrint({
    res <- model_results()
    if (is.character(res)) {
      cat("ERROR:\n", res)
    } else {
      cat("Success! Results calculated and displayed below.")
    }
  })
  
  # Output for the final prediction table
  output$prediction_table <- renderTable({
    res <- model_results()
    if (is.data.frame(res)) {
      return(res)
    } else {
      return(NULL)
    }
  }, striped = TRUE, bordered = TRUE, hover = TRUE)
}

# Run the application
shinyApp(ui = ui, server = server)