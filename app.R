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
# MODEL SETTINGS
# ======================================================================

RECENCY_HALFLIFE_DAYS <- 14
PRIOR_GAMES <- 8
MAX_GOALS_PROB <- 15
MIN_TEAM_GAMES_FOR_MODEL <- 20
CACHING_DAYS <- 90
SCHEDULE_INITIAL_LOOKAHEAD_DAYS <- 7
SCHEDULE_MAX_LOOKAHEAD_DAYS <- 45
SCHEDULE_LOOKAHEAD_STEP_DAYS <- 7

# ======================================================================
# PART 1: DATA SCRAPING & HELPERS
# ======================================================================

get_espn_scoreboard <- function(date) {
  url <- paste0("https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard?dates=", date)
  res <- httr::GET(url)
  stop_for_status(res)
  jsonlite::fromJSON(content(res, "text", encoding = "UTF-8"), flatten = FALSE)
}

parse_espn_datetime <- function(x) {
  if (is.null(x) || length(x) == 0 || is.na(x[[1]]) || x[[1]] == "") {
    return(as.POSIXct(NA))
  }

  raw <- as.character(x[[1]])

  # ESPN may send varying ISO-like time formats depending on endpoint context.
  parsed <- suppressWarnings(ymd_hms(raw, tz = "UTC", quiet = TRUE))
  if (is.na(parsed)) parsed <- suppressWarnings(ymd_hm(raw, tz = "UTC", quiet = TRUE))
  if (is.na(parsed)) parsed <- suppressWarnings(as.POSIXct(raw, format = "%Y-%m-%dT%H:%MZ", tz = "UTC"))
  if (is.na(parsed)) parsed <- suppressWarnings(as.POSIXct(raw, format = "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"))

  parsed
}

parse_total_from_text <- function(x) {
  if (is.null(x) || length(x) == 0 || is.na(x[[1]]) || x[[1]] == "") {
    return(NA_real_)
  }

  txt <- as.character(x[[1]])

  # Prefer explicit Over/Under markers when present.
  ou_match <- regexec("(?i)(?:o\\s*/\\s*u|over\\s*/\\s*under|total)\\s*([0-9]+(?:\\.[0-9]+)?)", txt, perl = TRUE)
  pieces <- regmatches(txt, ou_match)[[1]]
  if (length(pieces) >= 2) {
    value <- suppressWarnings(as.numeric(pieces[2]))
    if (is.finite(value) && value >= 2 && value <= 15) return(value)
  }

  # Fallback: first plausible total-like number in expected NHL range.
  all_nums <- gregexpr("[0-9]+(?:\\.[0-9]+)?", txt, perl = TRUE)
  raw_vals <- regmatches(txt, all_nums)[[1]]
  if (length(raw_vals) == 0) return(NA_real_)
  vals <- suppressWarnings(as.numeric(raw_vals))
  vals <- vals[is.finite(vals) & vals >= 2 & vals <= 15]
  if (length(vals) == 0) return(NA_real_)
  vals[1]
}

extract_vegas_total_from_comp <- function(comp) {
  parse_total_candidate <- function(candidate) {
    if (is.null(candidate) || length(candidate) == 0) return(NA_real_)

    numeric_direct <- suppressWarnings(as.numeric(candidate[[1]]))
    if (is.finite(numeric_direct) && numeric_direct >= 2 && numeric_direct <= 15) {
      return(numeric_direct)
    }

    parse_total_from_text(candidate[[1]])
  }

  # Try flattened columns first.
  flat_candidates <- c("odds.overUnder", "odds.total", "overUnder", "total")
  for (nm in flat_candidates) {
    if (!is.null(comp[[nm]])) {
      value <- parse_total_candidate(comp[[nm]])
      if (is.finite(value)) return(value)
    }
  }

  odds_obj <- comp$odds
  if (is.null(odds_obj)) return(NA_real_)

  if (is.data.frame(odds_obj) && nrow(odds_obj) > 0) {
    for (nm in c("overUnder", "total", "details")) {
      if (!is.null(odds_obj[[nm]])) {
        value <- parse_total_candidate(odds_obj[[nm]])
        if (is.finite(value)) return(value)
      }
    }
  }

  if (is.list(odds_obj)) {
    for (nm in c("overUnder", "total", "details")) {
      if (!is.null(odds_obj[[nm]])) {
        value <- parse_total_candidate(odds_obj[[nm]])
        if (is.finite(value)) return(value)
      }
    }
  }

  NA_real_
}

extract_games_from_json <- function(js) {
  events <- js$events
  if (is.null(events) || !is.data.frame(events) || nrow(events) == 0) {
    return(NULL)
  }

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
      date = as.Date(substr(game$date %||% NA, 1, 10)),
      home_team = home_team_name,
      away_team = away_team_name,
      home_goals = home_score,
      away_goals = away_score,
      stringsAsFactors = FALSE
    )
  })
}

extract_schedule_from_json <- function(js) {
  events <- js$events
  if (is.null(events) || !is.data.frame(events) || nrow(events) == 0) {
    return(NULL)
  }

  events_list <- purrr::transpose(events)

  get_status_field <- function(status_obj, nested_name, flat_name, default = NA) {
    if (is.null(status_obj)) return(default)

    # Sometimes ESPN returns nested `status$type`, sometimes flattened `type.*` columns.
    if (is.list(status_obj) && !is.data.frame(status_obj) && !is.null(status_obj$type)) {
      value <- status_obj$type[[nested_name]] %||% default
      if (length(value) == 0) return(default)
      return(value[[1]])
    }

    if (is.data.frame(status_obj)) {
      value <- status_obj[[flat_name]] %||% status_obj[[nested_name]] %||% default
      if (length(value) == 0) return(default)
      return(value[[1]])
    }

    if (is.atomic(status_obj) && length(status_obj) == 1 && !is.na(status_obj)) {
      return(status_obj[[1]])
    }

    default
  }

  map_dfr(events_list, function(game) {
    if (is.null(game$competitions) || length(game$competitions) == 0) return(NULL)
    comp <- game$competitions[1, ]
    if (is.null(comp$competitors) || length(comp$competitors) == 0) return(NULL)
    competitors <- comp$competitors[[1]]
    home <- dplyr::filter(competitors, homeAway == "home")
    away <- dplyr::filter(competitors, homeAway == "away")
    if (nrow(home) != 1 || nrow(away) != 1) return(NULL)

    start_utc <- parse_espn_datetime(game$date %||% NA)
    if (is.na(start_utc)) {
      start_local <- as.POSIXct(NA)
      game_date <- as.Date(NA)
    } else {
      start_local <- with_tz(start_utc, tzone = Sys.timezone())
      game_date <- as.Date(start_local)
    }

    status_obj <- game$status
    completed <- as.logical(get_status_field(status_obj, "completed", "type.completed", FALSE))
    if (is.na(completed)) completed <- FALSE
    state <- as.character(get_status_field(status_obj, "state", "type.state", NA_character_))
    status_text <- as.character(get_status_field(status_obj, "description", "type.description", NA_character_))
    vegas_total <- extract_vegas_total_from_comp(comp)

    data.frame(
      game_id = game$id %||% NA,
      game_date = game_date,
      start_time_utc = start_utc,
      start_time_local = start_local,
      home_team = home$team$displayName %||% NA,
      away_team = away$team$displayName %||% NA,
      completed = completed,
      state = state,
      status_text = status_text,
      vegas_total = vegas_total,
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
        NULL
      }
    )
    if (is.null(js)) return(NULL)

    games_df <- tryCatch(
      extract_games_from_json(js),
      error = function(e) {
        suppressWarnings(message(paste("Error extracting games for", date, ":", conditionMessage(e))))
        NULL
      }
    )
    games_df
  })

  bind_rows(all_games_list)
}

get_nhl_schedule_for_dates <- function(dates) {
  if (!is.vector(dates) || !is.character(dates)) {
    stop("Input 'dates' must be a vector of dates in 'YYYYMMDD' character format.")
  }

  all_schedule_list <- purrr::map(dates, function(date) {
    message(paste("Fetching schedule for:", date))
    js <- tryCatch(
      get_espn_scoreboard(date),
      error = function(e) {
        suppressWarnings(message(paste("Error fetching schedule for", date, ":", conditionMessage(e))))
        NULL
      }
    )
    if (is.null(js)) return(NULL)

    schedule_df <- tryCatch(
      extract_schedule_from_json(js),
      error = function(e) {
        suppressWarnings(message(paste("Error extracting schedule for", date, ":", conditionMessage(e))))
        NULL
      }
    )
    schedule_df
  })

  schedule_df <- bind_rows(all_schedule_list)
  if (is.null(schedule_df) || nrow(schedule_df) == 0) {
    return(data.frame(
      game_id = character(),
      game_date = as.Date(character()),
      start_time_utc = as.POSIXct(character()),
      start_time_local = as.POSIXct(character()),
      home_team = character(),
      away_team = character(),
      completed = logical(),
      state = character(),
      status_text = character(),
      vegas_total = numeric(),
      stringsAsFactors = FALSE
    ))
  }

  schedule_df %>%
    dplyr::filter(!is.na(game_id), !is.na(home_team), !is.na(away_team)) %>%
    dplyr::distinct(game_id, .keep_all = TRUE) %>%
    dplyr::arrange(start_time_utc)
}

fetch_game_vegas_total <- function(game_id, game_date) {
  if (is.null(game_id) || length(game_id) == 0 || is.na(game_id[[1]])) return(NA_real_)
  if (is.null(game_date) || length(game_date) == 0 || is.na(game_date[[1]])) return(NA_real_)

  date_str <- format(as.Date(game_date[[1]]), "%Y%m%d")
  js <- tryCatch(
    get_espn_scoreboard(date_str),
    error = function(e) NULL
  )
  if (is.null(js)) return(NA_real_)

  fresh <- tryCatch(
    extract_schedule_from_json(js),
    error = function(e) NULL
  )
  if (is.null(fresh) || nrow(fresh) == 0) return(NA_real_)

  value <- fresh %>%
    dplyr::filter(game_id == as.character(game_id[[1]])) %>%
    dplyr::slice(1) %>%
    dplyr::pull(vegas_total)

  if (length(value) == 0) return(NA_real_)
  as.numeric(value[[1]])
}

normalize_team_name <- function(team_name) {
  if (is.null(team_name) || length(team_name) == 0 || is.na(team_name[[1]])) return(NA_character_)
  value <- as.character(team_name[[1]])
  value <- iconv(value, to = "ASCII//TRANSLIT")
  value <- tolower(gsub("[^a-z0-9]", "", value))

  # Unify known naming variants across feeds.
  if (value %in% c("utahhockeyclub", "utahmammoth")) value <- "utah"

  value
}

fetch_total_from_odds_api <- function(home_team, away_team, game_date, return_meta = FALSE) {
  result <- list(total = NA_real_, reason = "unknown", book = NA_character_)

  api_key <- Sys.getenv("ODDS_API_KEY", "")
  if (api_key == "") {
    result$reason <- "ODDS_API_KEY is not set."
    return(if (return_meta) result else result$total)
  }
  if (is.null(game_date) || length(game_date) == 0 || is.na(game_date[[1]])) {
    result$reason <- "Selected game has no valid date."
    return(if (return_meta) result else result$total)
  }

  api_get_json <- function(url, query) {
    res <- tryCatch(httr::GET(url, query = query), error = function(e) NULL)
    if (is.null(res)) {
      return(list(ok = FALSE, status = NA_integer_, body = NULL, reason = "request failed before receiving a response"))
    }
    status <- httr::status_code(res)
    if (status != 200) {
      return(list(ok = FALSE, status = status, body = NULL, reason = paste("HTTP", status)))
    }
    body <- tryCatch(
      jsonlite::fromJSON(httr::content(res, "text", encoding = "UTF-8"), simplifyVector = FALSE),
      error = function(e) NULL
    )
    if (is.null(body)) {
      return(list(ok = FALSE, status = status, body = NULL, reason = "invalid JSON response"))
    }
    list(ok = TRUE, status = status, body = body, reason = "ok")
  }

  american_to_prob <- function(price) {
    if (!is.finite(price)) return(NA_real_)
    if (price < 0) return((-price) / ((-price) + 100))
    100 / (price + 100)
  }

  extract_total_quotes_from_bookmakers <- function(bookmakers) {
    if (!is.list(bookmakers) || length(bookmakers) == 0) return(data.frame())
    quotes <- list()
    for (book in bookmakers) {
      book_key_raw <- book$key %||% ""
      if (length(book_key_raw) == 0) book_key_raw <- ""
      book_key <- as.character(book_key_raw[[1]])
      book_title_raw <- book$title %||% book_key
      if (length(book_title_raw) == 0) book_title_raw <- book_key
      book_title <- as.character(book_title_raw[[1]])
      markets <- book$markets
      if (!is.list(markets) || length(markets) == 0) next
      for (market in markets) {
        market_key_raw <- market$key %||% ""
        if (length(market_key_raw) == 0) market_key_raw <- ""
        market_key <- as.character(market_key_raw[[1]])
        # Use main totals market only to avoid alternate-line contamination.
        if (!isTRUE(market_key %in% c("totals"))) next
        outcomes <- market$outcomes
        if (!is.list(outcomes) || length(outcomes) == 0) next
        for (outcome in outcomes) {
          pt_raw <- outcome$point %||% NA_real_
          if (length(pt_raw) == 0) pt_raw <- NA_real_
          pt <- suppressWarnings(as.numeric(pt_raw[[1]]))

          side_raw <- outcome$name %||% NA_character_
          if (length(side_raw) == 0) side_raw <- NA_character_
          side <- tolower(as.character(side_raw[[1]]))

          price_raw <- outcome$price %||% NA_real_
          if (length(price_raw) == 0) price_raw <- NA_real_
          price <- suppressWarnings(as.numeric(price_raw[[1]]))

          if (!is.finite(pt) || pt < 2 || pt > 15) next
          if (!isTRUE(side %in% c("over", "under"))) next

          quotes[[length(quotes) + 1]] <- data.frame(
            book_key = book_key,
            book_title = book_title,
            market_key = market_key,
            point = pt,
            side = side,
            price = price,
            stringsAsFactors = FALSE
          )
        }
      }
    }
    if (length(quotes) == 0) return(data.frame())
    dplyr::bind_rows(quotes)
  }

  choose_half_goal_total <- function(quotes_df) {
    if (is.null(quotes_df) || nrow(quotes_df) == 0) return(NA_real_)

    frac <- abs(quotes_df$point - floor(quotes_df$point))
    half_quotes <- quotes_df[abs(frac - 0.5) < 1e-8, , drop = FALSE]
    if (nrow(half_quotes) > 0) {
      return(stats::median(half_quotes$point))
    }

    # If only integer points exist, infer nearest .5 line from implied over/under balance.
    point_summary <- quotes_df %>%
      dplyr::mutate(implied_prob = vapply(price, american_to_prob, numeric(1))) %>%
      dplyr::filter(is.finite(implied_prob)) %>%
      dplyr::group_by(point) %>%
      dplyr::summarise(
        over_prob_raw = mean(implied_prob[side == "over"], na.rm = TRUE),
        under_prob_raw = mean(implied_prob[side == "under"], na.rm = TRUE),
        n_over = sum(side == "over"),
        n_under = sum(side == "under"),
        n = n(),
        .groups = "drop"
      ) %>%
      dplyr::mutate(
        has_both = n_over > 0 & n_under > 0,
        over_prob_fair = dplyr::if_else(
          has_both & (over_prob_raw + under_prob_raw) > 0,
          over_prob_raw / (over_prob_raw + under_prob_raw),
          dplyr::if_else(n_over > 0, over_prob_raw, 1 - under_prob_raw)
        ),
        dist_to_even = abs(over_prob_fair - 0.5)
      ) %>%
      dplyr::arrange(dist_to_even, dplyr::desc(n))

    if (nrow(point_summary) == 0) return(NA_real_)

    best <- point_summary[1, ]
    base_point <- as.numeric(best$point)
    over_prob <- as.numeric(best$over_prob_fair)
    if (!is.finite(base_point) || !is.finite(over_prob)) return(NA_real_)

    half_line <- ifelse(over_prob >= 0.5, base_point + 0.5, base_point - 0.5)
    half_line <- pmin(pmax(half_line, 2.5), 14.5)
    half_line
  }

  pick_book_total <- function(quotes_df) {
    if (is.null(quotes_df) || nrow(quotes_df) == 0) {
      return(list(total = NA_real_, book = NA_character_))
    }

    quotes_df$book_key <- tolower(as.character(quotes_df$book_key))
    quotes_df$book_title <- as.character(quotes_df$book_title)
    missing_title <- is.na(quotes_df$book_title) | quotes_df$book_title == ""
    quotes_df$book_title[missing_title] <- quotes_df$book_key[missing_title]

    fanduel_quotes <- quotes_df[quotes_df$book_key == "fanduel", , drop = FALSE]
    if (nrow(fanduel_quotes) > 0) {
      fanduel_total <- choose_half_goal_total(fanduel_quotes)
      if (is.finite(fanduel_total)) {
        return(list(total = fanduel_total, book = "FanDuel"))
      }
    }

    fallback_priority <- c(
      "draftkings",
      "betmgm",
      "caesars",
      "betrivers",
      "espnbet",
      "fanatics",
      "hardrockbet",
      "sportsbet",
      "tabtouch",
      "williamhill",
      "betfair_ex_uk",
      "betfair_ex_au",
      "onexbet",
      "marathonbet"
    )

    grouped <- split(quotes_df, quotes_df$book_key, drop = TRUE)
    fallback <- lapply(names(grouped), function(book_key) {
      q <- grouped[[book_key]]
      total <- choose_half_goal_total(q)
      title <- unique(q$book_title)
      title <- title[!is.na(title) & nzchar(title)]
      if (length(title) == 0) title <- book_key
      rank <- match(book_key, fallback_priority)
      if (is.na(rank)) rank <- length(fallback_priority) + 1
      data.frame(
        book_key = as.character(book_key),
        book_title = as.character(title[[1]]),
        total = as.numeric(total),
        n_quotes = nrow(q),
        rank = as.integer(rank),
        stringsAsFactors = FALSE
      )
    })

    fallback_df <- dplyr::bind_rows(fallback) %>%
      dplyr::filter(is.finite(total)) %>%
      dplyr::arrange(rank, dplyr::desc(n_quotes), book_title)

    if (nrow(fallback_df) == 0) {
      return(list(total = NA_real_, book = NA_character_))
    }

    list(
      total = as.numeric(fallback_df$total[[1]]),
      book = as.character(fallback_df$book_title[[1]])
    )
  }

  primary_regions <- "us"
  fallback_regions <- "us,uk,eu,au"
  target_date <- as.Date(game_date[[1]])
  target_home <- normalize_team_name(home_team)
  target_away <- normalize_team_name(away_team)

  # 1) Find event via events endpoint (more reliable matching payload).
  events_resp <- api_get_json(
    "https://api.the-odds-api.com/v4/sports/icehockey_nhl/events",
    query = list(apiKey = api_key, dateFormat = "iso")
  )
  if (!events_resp$ok || !is.list(events_resp$body) || length(events_resp$body) == 0) {
    result$reason <- paste("events endpoint failed:", events_resp$reason)
    return(if (return_meta) result else result$total)
  }

  events <- events_resp$body
  event <- purrr::detect(events, function(evt) {
    evt_home <- normalize_team_name(evt$home_team %||% NA_character_)
    evt_away <- normalize_team_name(evt$away_team %||% NA_character_)
    evt_time <- parse_espn_datetime(evt$commence_time %||% NA)
    evt_date <- as.Date(evt_time)
    teams_match <- (
      identical(evt_home, target_home) && identical(evt_away, target_away)
    ) || (
      identical(evt_home, target_away) && identical(evt_away, target_home)
    )
    date_match <- !is.na(evt_date) && !is.na(target_date) && abs(as.numeric(evt_date - target_date)) <= 1
    teams_match && date_match
  })

  if (is.null(event) || is.null(event$id) || length(event$id) == 0) {
    result$reason <- "No matching event found in Odds API events endpoint."
    return(if (return_meta) result else result$total)
  }

  fetch_event_quotes <- function(regions_value) {
    odds_resp <- api_get_json(
      paste0("https://api.the-odds-api.com/v4/sports/icehockey_nhl/events/", event_id, "/odds"),
      query = list(
        apiKey = api_key,
        regions = regions_value,
        # Some plans return totals only when h2h is also requested.
        markets = "h2h,totals",
        oddsFormat = "american",
        dateFormat = "iso"
      )
    )
    if (!odds_resp$ok || is.null(odds_resp$body)) {
      return(list(quotes = data.frame(), reason = paste("event odds endpoint failed:", odds_resp$reason)))
    }
    quotes <- extract_total_quotes_from_bookmakers(odds_resp$body$bookmakers)
    list(quotes = quotes, reason = "ok")
  }

  fetch_sport_quotes <- function(regions_value) {
    sport_resp <- api_get_json(
      "https://api.the-odds-api.com/v4/sports/icehockey_nhl/odds",
      query = list(
        apiKey = api_key,
        regions = regions_value,
        markets = "h2h,totals",
        oddsFormat = "american",
        dateFormat = "iso"
      )
    )
    if (!sport_resp$ok || !is.list(sport_resp$body) || length(sport_resp$body) == 0) {
      return(data.frame())
    }
    sport_event <- purrr::detect(sport_resp$body, function(evt) {
      as.character(evt$id %||% "") == event_id
    })
    if (is.null(sport_event)) return(data.frame())
    extract_total_quotes_from_bookmakers(sport_event$bookmakers)
  }

  # 2) Pull odds for matched event. Try US first, then broaden regions.
  event_id <- as.character(event$id[[1]])
  query_regions <- unique(c(primary_regions, fallback_regions))
  quotes <- data.frame()
  last_reason <- "no quotes"
  for (reg in query_regions) {
    eq <- fetch_event_quotes(reg)
    last_reason <- eq$reason
    if (is.data.frame(eq$quotes) && nrow(eq$quotes) > 0) {
      quotes <- eq$quotes
      break
    }
  }
  if (!is.data.frame(quotes) || nrow(quotes) == 0) {
    for (reg in query_regions) {
      sq <- fetch_sport_quotes(reg)
      if (is.data.frame(sq) && nrow(sq) > 0) {
        quotes <- sq
        break
      }
    }
  }

  if (!is.data.frame(quotes) || nrow(quotes) == 0) {
    result$reason <- paste0(
      "Matched event but no totals quotes from Odds API. Last response: ",
      last_reason,
      ". Checked regions: ",
      paste(query_regions, collapse = ", "),
      "."
    )
    return(if (return_meta) result else result$total)
  }

  selected_total <- pick_book_total(quotes)
  if (!is.finite(selected_total$total)) {
    result$reason <- "Unable to derive a sportsbook total from available odds quotes."
    return(if (return_meta) result else result$total)
  }

  result$total <- selected_total$total
  result$book <- selected_total$book
  result$reason <- "ok"
  if (return_meta) return(result)
  result$total
}

generate_date_range <- function(start_date, end_date) {
  dates <- seq(
    as.Date(start_date, format = "%Y%m%d"),
    as.Date(end_date, format = "%Y%m%d"),
    by = "day"
  )
  format(dates, "%Y%m%d")
}

build_schedule_card_choices <- function(schedule_df) {
  if (is.null(schedule_df) || nrow(schedule_df) == 0) {
    return(list(choice_names = list(), choice_values = character()))
  }

  choice_names <- purrr::map(seq_len(nrow(schedule_df)), function(i) {
    row <- schedule_df[i, ]
    game_date <- if (!is.na(row$game_date)) {
      row$game_date
    } else if (!is.na(row$start_time_local)) {
      as.Date(row$start_time_local)
    } else {
      as.Date(NA)
    }
    date_label <- if (!is.na(game_date)) format(game_date, "%m/%d/%Y") else "TBD"

    tags$div(
      class = "game-card",
      tags$div(class = "game-card-date", date_label),
      tags$div(class = "game-card-matchup", paste0(row$away_team, " at ", row$home_team))
    )
  })

  list(
    choice_names = choice_names,
    choice_values = schedule_df$game_id
  )
}

empty_schedule_df <- function() {
  data.frame(
    game_id = character(),
    game_date = as.Date(character()),
    start_time_utc = as.POSIXct(character()),
    start_time_local = as.POSIXct(character()),
    home_team = character(),
    away_team = character(),
    completed = logical(),
    state = character(),
    status_text = character(),
    vegas_total = numeric(),
    stringsAsFactors = FALSE
  )
}

fetch_upcoming_schedule <- function(
  start_date = Sys.Date(),
  initial_days = SCHEDULE_INITIAL_LOOKAHEAD_DAYS,
  max_days = SCHEDULE_MAX_LOOKAHEAD_DAYS,
  step_days = SCHEDULE_LOOKAHEAD_STEP_DAYS
) {
  lookahead_days <- initial_days
  last_window <- empty_schedule_df()
  end_date <- start_date + initial_days

  while (lookahead_days <= max_days) {
    end_date <- start_date + lookahead_days
    dates <- generate_date_range(format(start_date, "%Y%m%d"), format(end_date, "%Y%m%d"))

    cat(paste("CACHING: Scraping NHL schedule from", start_date, "to", end_date, "\n"))
    schedule <- get_nhl_schedule_for_dates(dates) %>%
      dplyr::mutate(game_date = dplyr::coalesce(game_date, as.Date(start_time_local))) %>%
      dplyr::filter(!completed, !is.na(game_date), game_date >= start_date) %>%
      dplyr::arrange(game_date, start_time_local)

    last_window <- schedule
    if (nrow(schedule) > 0) {
      return(list(schedule = schedule, lookahead_days = lookahead_days, end_date = end_date))
    }

    lookahead_days <- lookahead_days + step_days
  }

  list(schedule = last_window, lookahead_days = max_days, end_date = end_date)
}

# ======================================================================
# CACHING SECTION
# ======================================================================

END_DATE <- Sys.Date()
START_DATE <- END_DATE - CACHING_DAYS
CACHED_DATES <- generate_date_range(format(START_DATE, "%Y%m%d"), format(END_DATE, "%Y%m%d"))

cat(paste("CACHING: Scraping", CACHING_DAYS, "days of historical data from", START_DATE, "to", END_DATE, "\n"))
CACHED_HISTORICAL_DATA <- get_nhl_data_for_dates(CACHED_DATES)

if (is.null(CACHED_HISTORICAL_DATA) || nrow(CACHED_HISTORICAL_DATA) == 0) {
  cat("WARNING: Historical caching failed. CACHED_HISTORICAL_DATA is empty.\n")
  CACHED_HISTORICAL_DATA <- data.frame(
    game_id = character(),
    date = as.Date(character()),
    home_team = character(),
    away_team = character(),
    home_goals = numeric(),
    away_goals = numeric(),
    stringsAsFactors = FALSE
  )
} else {
  cat(paste("CACHING: Successfully cached", nrow(CACHED_HISTORICAL_DATA), "games.\n"))
}

SCHEDULE_START <- Sys.Date()
schedule_fetch <- fetch_upcoming_schedule(
  start_date = SCHEDULE_START,
  initial_days = SCHEDULE_INITIAL_LOOKAHEAD_DAYS,
  max_days = SCHEDULE_MAX_LOOKAHEAD_DAYS,
  step_days = SCHEDULE_LOOKAHEAD_STEP_DAYS
)
UPCOMING_SCHEDULE <- schedule_fetch$schedule
SCHEDULE_LOOKAHEAD_USED <- schedule_fetch$lookahead_days
SCHEDULE_WINDOW_END <- schedule_fetch$end_date

if (is.null(UPCOMING_SCHEDULE) || nrow(UPCOMING_SCHEDULE) == 0) {
  cat("WARNING: No upcoming games found in schedule cache.\n")
  UPCOMING_SCHEDULE <- empty_schedule_df()
} else {
  cat(paste("CACHING: Successfully cached", nrow(UPCOMING_SCHEDULE), "upcoming games.\n"))
}

CACHED_UNTIL <- format(END_DATE, "%B %d, %Y")
SCHEDULE_CARD_CHOICES <- build_schedule_card_choices(UPCOMING_SCHEDULE)
DEFAULT_GAME <- if (nrow(UPCOMING_SCHEDULE) > 0) UPCOMING_SCHEDULE$game_id[1] else ""

# ======================================================================
# PART 2: MODEL FUNCTIONS
# ======================================================================

build_stacked_games <- function(df) {
  home_data <- df %>%
    dplyr::mutate(is_home = TRUE) %>%
    dplyr::select(
      game_id, date,
      goals_scored = home_goals, goals_conceded = away_goals,
      team = home_team, opponent = away_team, is_home
    )
  away_data <- df %>%
    dplyr::mutate(is_home = FALSE) %>%
    dplyr::select(
      game_id, date,
      goals_scored = away_goals, goals_conceded = home_goals,
      team = away_team, opponent = home_team, is_home
    )

  bind_rows(home_data, away_data) %>%
    dplyr::filter(
      !is.na(goals_scored),
      !is.na(goals_conceded),
      !is.na(team),
      !is.na(opponent),
      team != "",
      opponent != ""
    )
}

compute_team_strengths <- function(stacked_df, prior_games = PRIOR_GAMES) {
  if (nrow(stacked_df) == 0) {
    stop("No completed games found to compute team strengths.")
  }

  league_mu <- mean(stacked_df$goals_scored, na.rm = TRUE)
  if (!is.finite(league_mu) || league_mu <= 0) league_mu <- 3

  team_stats <- stacked_df %>%
    dplyr::group_by(team) %>%
    dplyr::summarise(
      games = n(),
      goals_for = sum(goals_scored, na.rm = TRUE),
      goals_against = sum(goals_conceded, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    dplyr::mutate(
      smoothed_for = (goals_for + prior_games * league_mu) / (games + prior_games),
      smoothed_against = (goals_against + prior_games * league_mu) / (games + prior_games),
      attack_ratio = smoothed_for / league_mu,
      defense_ratio = smoothed_against / league_mu
    )

  home_mu <- mean(stacked_df$goals_scored[stacked_df$is_home], na.rm = TRUE)
  away_mu <- mean(stacked_df$goals_scored[!stacked_df$is_home], na.rm = TRUE)
  home_adv <- ifelse(is.finite(away_mu) && away_mu > 0, home_mu / away_mu, 1)
  home_adv <- pmin(pmax(home_adv, 0.8), 1.25)

  list(
    league_mu = league_mu,
    home_adv = home_adv,
    team_stats = team_stats
  )
}

lookup_team_stat <- function(team_stats, team_name, column_name, default_value) {
  row_idx <- which(team_stats$team == team_name)
  if (length(row_idx) == 0) return(default_value)

  value <- team_stats[[column_name]][row_idx[1]]
  if (is.na(value) || !is.finite(value)) return(default_value)
  value
}

build_count_model <- function(df) {
  if (is.null(df) || nrow(df) == 0) {
    stop("Input data frame is empty or NULL. Cannot train model.")
  }

  stacked_df <- build_stacked_games(df)
  if (nrow(stacked_df) < MIN_TEAM_GAMES_FOR_MODEL) {
    stop(
      paste(
        "Insufficient completed games to train model. Need at least",
        MIN_TEAM_GAMES_FOR_MODEL,
        "team-games."
      )
    )
  }

  latest_date <- max(stacked_df$date, na.rm = TRUE)
  days_ago <- as.numeric(latest_date - stacked_df$date)
  recency_weight <- 0.5^(days_ago / RECENCY_HALFLIFE_DAYS)
  recency_weight[!is.finite(recency_weight)] <- 1

  strengths <- compute_team_strengths(stacked_df)

  nb_model <- tryCatch(
    MASS::glm.nb(
      goals_scored ~ team + opponent + is_home,
      data = stacked_df,
      weights = recency_weight,
      control = glm.control(maxit = 100)
    ),
    error = function(e) NULL
  )

  list(
    nb_model = nb_model,
    theta = if (!is.null(nb_model) && is.finite(nb_model$theta)) nb_model$theta else NA_real_,
    strengths = strengths
  )
}

predict_strength_based <- function(model_obj, home_team, away_team) {
  strengths <- model_obj$strengths
  team_stats <- strengths$team_stats

  home_attack <- lookup_team_stat(team_stats, home_team, "attack_ratio", 1)
  away_attack <- lookup_team_stat(team_stats, away_team, "attack_ratio", 1)
  home_defense <- lookup_team_stat(team_stats, home_team, "defense_ratio", 1)
  away_defense <- lookup_team_stat(team_stats, away_team, "defense_ratio", 1)
  home_games <- lookup_team_stat(team_stats, home_team, "games", 0)
  away_games <- lookup_team_stat(team_stats, away_team, "games", 0)

  home_lambda <- strengths$league_mu * strengths$home_adv * home_attack * away_defense
  away_lambda <- strengths$league_mu * (1 / strengths$home_adv) * away_attack * home_defense

  # Keep sparse-sample predictions in a realistic range.
  home_lambda <- pmin(pmax(home_lambda, 0.4), 8.5)
  away_lambda <- pmin(pmax(away_lambda, 0.4), 8.5)

  reliability <- pmin(1, min(home_games, away_games) / 12)

  list(
    home_lambda = home_lambda,
    away_lambda = away_lambda,
    reliability = reliability,
    home_games = home_games,
    away_games = away_games
  )
}

predict_expected_goals <- function(model_obj, home_team, away_team) {
  baseline <- predict_strength_based(model_obj, home_team, away_team)

  use_nb <- FALSE
  nb_home <- NA_real_
  nb_away <- NA_real_

  if (!is.null(model_obj$nb_model)) {
    predict_df <- data.frame(
      team = factor(c(home_team, away_team), levels = model_obj$nb_model$xlevels$team),
      opponent = factor(c(away_team, home_team), levels = model_obj$nb_model$xlevels$opponent),
      is_home = c(TRUE, FALSE)
    )

    nb_lambda <- tryCatch(
      predict(model_obj$nb_model, predict_df, type = "response"),
      error = function(e) c(NA_real_, NA_real_)
    )

    if (length(nb_lambda) == 2 && all(is.finite(nb_lambda)) && all(nb_lambda > 0)) {
      use_nb <- TRUE
      nb_home <- nb_lambda[1]
      nb_away <- nb_lambda[2]
    }
  }

  blend_weight <- ifelse(use_nb, baseline$reliability, 0)
  home_lambda <- blend_weight * nb_home + (1 - blend_weight) * baseline$home_lambda
  away_lambda <- blend_weight * nb_away + (1 - blend_weight) * baseline$away_lambda

  list(
    home_lambda = home_lambda,
    away_lambda = away_lambda,
    theta = model_obj$theta,
    blend_weight = blend_weight,
    home_games = baseline$home_games,
    away_games = baseline$away_games
  )
}

calculate_over_under_prob <- function(home_lambda, away_lambda, vegas_total, theta = NA_real_) {
  goals <- 0:MAX_GOALS_PROB

  use_nb <- is.finite(theta) && theta > 0
  home_probs <- if (use_nb) {
    dnbinom(goals, mu = home_lambda, size = theta)
  } else {
    dpois(goals, home_lambda)
  }
  away_probs <- if (use_nb) {
    dnbinom(goals, mu = away_lambda, size = theta)
  } else {
    dpois(goals, away_lambda)
  }

  prob_matrix <- outer(home_probs, away_probs)
  total_goals_matrix <- outer(goals, goals, FUN = `+`)

  prob_under <- sum(prob_matrix[total_goals_matrix < vegas_total])
  prob_over <- sum(prob_matrix[total_goals_matrix > vegas_total])
  prob_push <- sum(prob_matrix[total_goals_matrix == vegas_total])
  total_prob <- prob_under + prob_over + prob_push

  list(
    prob_under = prob_under / total_prob,
    prob_over = prob_over / total_prob,
    prob_push = prob_push / total_prob,
    expected_total_goals = home_lambda + away_lambda,
    scoring_dist = ifelse(use_nb, "Negative Binomial", "Poisson")
  )
}

# ======================================================================
# PART 3: MASTER WRAPPER FUNCTION
# ======================================================================

run_ou_prediction <- function(home_team, away_team, vegas_total, filtered_data, line_source = "Unknown") {
  if (is.null(filtered_data) || nrow(filtered_data) < 10) {
    stop(
      paste(
        "Insufficient data (scraped:",
        nrow(filtered_data),
        "games) to train a reliable model. Select a different historical window."
      )
    )
  }

  message("Training recency-weighted model...")
  nhl_model <- build_count_model(filtered_data)
  lambdas <- predict_expected_goals(nhl_model, home_team, away_team)

  probabilities <- calculate_over_under_prob(
    lambdas$home_lambda,
    lambdas$away_lambda,
    vegas_total = vegas_total,
    theta = lambdas$theta
  )

  lean <- dplyr::case_when(
    probabilities$prob_over > probabilities$prob_under ~ "OVER",
    probabilities$prob_under > probabilities$prob_over ~ "UNDER",
    TRUE ~ "NO EDGE"
  )

  edge_strength <- abs(probabilities$prob_over - probabilities$prob_under)
  matchup <- paste(away_team, "at", home_team)
  prediction_label <- ifelse(lean == "NO EDGE", "NO EDGE", paste(lean, vegas_total))

  details_df <- data.frame(
    Metric = c(
      "Matchup",
      "Vegas Line",
      "Vegas Line Source",
      "Predicted Home Goals",
      "Predicted Away Goals",
      "Model Expected Total",
      "Probability OVER",
      "Probability UNDER",
      "Probability PUSH",
      "Model Lean",
      "Edge Strength |Over-Under|",
      "Model Blend Weight (NB)",
      "Sample Size (Home Team Games)",
      "Sample Size (Away Team Games)",
      "Scoring Distribution"
    ),
    Value = c(
      matchup,
      vegas_total,
      line_source,
      round(lambdas$home_lambda, 3),
      round(lambdas$away_lambda, 3),
      round(probabilities$expected_total_goals, 3),
      round(probabilities$prob_over, 4),
      round(probabilities$prob_under, 4),
      round(probabilities$prob_push, 4),
      lean,
      round(edge_strength, 4),
      round(lambdas$blend_weight, 3),
      lambdas$home_games,
      lambdas$away_games,
      probabilities$scoring_dist
    ),
    stringsAsFactors = FALSE
  )

  list(
    matchup = matchup,
    vegas_total = vegas_total,
    line_source = line_source,
    prediction_label = prediction_label,
    lean = lean,
    expected_total = probabilities$expected_total_goals,
    prob_over = probabilities$prob_over,
    prob_under = probabilities$prob_under,
    prob_push = probabilities$prob_push,
    edge_strength = edge_strength,
    details = details_df
  )
}

# ======================================================================
# PART 4: SHINY APPLICATION UI AND SERVER
# ======================================================================

ui <- fluidPage(
  tags$head(
    tags$style(HTML("
      #game_id.shiny-options-group {
        display: grid !important;
        grid-template-columns: repeat(auto-fit, minmax(340px, 1fr));
        gap: 12px;
      }
      @media (max-width: 1200px) {
        #game_id.shiny-options-group {
          grid-template-columns: repeat(2, minmax(0, 1fr));
        }
      }
      @media (max-width: 820px) {
        #game_id.shiny-options-group {
          grid-template-columns: repeat(1, minmax(0, 1fr));
        }
      }
      #game_id.shiny-options-group .radio {
        margin: 0 0 8px 0;
        padding: 0;
      }
      #game_id.shiny-options-group .radio input[type='radio'] {
        position: absolute;
        opacity: 0;
      }
      #game_id.shiny-options-group .radio label {
        display: block !important;
        margin: 0 !important;
        padding: 0 !important;
        width: 100% !important;
      }
      #game_id.shiny-options-group .radio .game-card {
        border: 1px solid #d0d7de;
        border-radius: 8px;
        background: #ffffff;
        padding: 14px 16px;
        cursor: pointer;
        height: 100%;
        transition: background-color 120ms ease, border-color 120ms ease, box-shadow 120ms ease;
      }
      #game_id.shiny-options-group .radio input[type='radio']:checked + .game-card,
      #game_id.shiny-options-group .radio input[type='radio']:checked ~ .game-card,
      #game_id.shiny-options-group .radio:has(input[type='radio']:checked) .game-card {
        border-color: #1f77b4;
        box-shadow: 0 0 0 3px rgba(31, 119, 180, 0.16);
        background: #eaf4ff;
      }
      #game_id.shiny-options-group .game-card-date {
        font-size: 12px;
        color: #57606a;
        margin-bottom: 4px;
      }
      #game_id.shiny-options-group .game-card-matchup {
        font-size: 15px;
        font-weight: 600;
        color: #1f2328;
      }
    "))
  ),
  titlePanel("NHL Over/Under Prediction Model"),
  sidebarLayout(
    sidebarPanel(
      radioButtons(
        "history_days",
        "Historical Data Window:",
        choices = c("Last 15 Days" = 15, "Last 30 Days" = 30, "Last 60 Days" = 60, "Last 90 Days" = 90),
        selected = 60
      ),
      hr(),
      p(
        tags$b("Technical Note:"),
        "Predictions blend a recency-weighted negative binomial model with smoothed team attack/defense rates."
      ),
      hr(),
      p(
        paste(
          "Historical game results are scraped once at app startup (up to",
          CACHING_DAYS,
          "days) and are current as of",
          CACHED_UNTIL,
          "."
        )
      ),
      p(
        paste(
          "Upcoming schedule search looked ahead",
          SCHEDULE_LOOKAHEAD_USED,
          "days (through",
          format(SCHEDULE_WINDOW_END, "%m/%d/%Y"),
          ")."
        )
      ),
      hr(),
      tags$i(
        HTML("<b>Disclaimer:</b> This model is for informational and entertainment purposes only. It is not financial advice or a guarantee of outcome. Do not use this information to bet.")
      )
    ),
    mainPanel(
      h3("Over/Under Prediction"),
      verbatimTextOutput("prediction_summary"),
      actionButton("toggle_details", "See underlying statistics", class = "btn-default"),
      br(),
      br(),
      tableOutput("prediction_table"),
      hr(),
      h3("Select Upcoming Game"),
      if (length(SCHEDULE_CARD_CHOICES$choice_values) > 0) {
        radioButtons(
          "game_id",
          label = NULL,
          choiceNames = SCHEDULE_CARD_CHOICES$choice_names,
          choiceValues = SCHEDULE_CARD_CHOICES$choice_values,
          selected = DEFAULT_GAME
        )
      } else {
        p("No upcoming scheduled games found in the current search window.")
      }
    )
  )
)

server <- function(input, output, session) {
  details_visible <- reactiveVal(FALSE)

  observeEvent(input$toggle_details, {
    details_visible(!details_visible())
  })

  observe({
    updateActionButton(
      session,
      "toggle_details",
      label = ifelse(details_visible(), "Hide underlying statistics", "See underlying statistics")
    )
  })

  observeEvent(list(input$game_id, input$history_days), {
    details_visible(FALSE)
  }, ignoreInit = TRUE)

  selected_game <- reactive({
    if (is.null(input$game_id) || input$game_id == "") {
      return(NULL)
    }
    UPCOMING_SCHEDULE %>%
      dplyr::filter(game_id == input$game_id) %>%
      dplyr::slice(1)
  })

  model_results <- reactive({
    req(input$game_id, input$history_days)

    selected <- selected_game()
    if (is.null(selected) || nrow(selected) == 0) {
      return("ERROR: No scheduled game selected. The schedule feed may be empty for this window.")
    }

    home_team <- selected$home_team[[1]]
    away_team <- selected$away_team[[1]]
    if (is.na(home_team) || is.na(away_team) || home_team == "" || away_team == "") {
      return("ERROR: Selected game is missing team data.")
    }

    line_source <- "Unavailable"
    odds_meta <- fetch_total_from_odds_api(
      home_team = home_team,
      away_team = away_team,
      game_date = selected$game_date[[1]],
      return_meta = TRUE
    )
    vegas_total <- odds_meta$total
    if (is.finite(vegas_total) && vegas_total >= 2 && vegas_total <= 15) {
      odds_book <- ""
      if (!is.null(odds_meta$book) && length(odds_meta$book) > 0 && !is.na(odds_meta$book[[1]])) {
        odds_book <- as.character(odds_meta$book[[1]])
      }
      line_source <- if (nzchar(odds_book)) odds_book else "The Odds API"
    }

    if (!is.finite(vegas_total) || vegas_total < 2 || vegas_total > 15) {
      line_source <- "ESPN (cached)"
      vegas_total <- suppressWarnings(as.numeric(selected$vegas_total[[1]]))
    }
    if (!is.finite(vegas_total) || vegas_total < 2 || vegas_total > 15) {
      line_source <- "ESPN (live refresh)"
      vegas_total <- fetch_game_vegas_total(selected$game_id[[1]], selected$game_date[[1]])
    }
    if (!is.finite(vegas_total) || vegas_total < 2 || vegas_total > 15) {
      return(
        paste(
          "ERROR: No sportsbook total is currently available.",
          paste0("Odds API: ", odds_meta$reason)
        )
      )
    }

    days_to_use <- as.numeric(input$history_days)
    filter_date <- Sys.Date() - days_to_use + 1

    filtered_data <- CACHED_HISTORICAL_DATA %>%
      dplyr::filter(
        date >= filter_date,
        !is.na(home_goals),
        !is.na(away_goals)
      )

    withProgress(message = "Calculating...", detail = "Filtering data and training model.", value = 0.5, {
      result <- tryCatch(
        run_ou_prediction(
          home_team = home_team,
          away_team = away_team,
          vegas_total = vegas_total,
          filtered_data = filtered_data,
          line_source = line_source
        ),
        error = function(e) as.character(e)
      )

      incProgress(1.0)
      result
    })
  })

  output$prediction_summary <- renderPrint({
    res <- model_results()

    if (is.character(res)) {
      cat(res)
      return(invisible(NULL))
    }

    cat(
      paste0("Prediction: ", res$prediction_label), "\n",
      paste0("Matchup: ", res$matchup), "\n",
      paste0("Model Expected Total: ", round(res$expected_total, 3)), "\n",
      paste0("P(OVER): ", round(res$prob_over, 4),
             " | P(UNDER): ", round(res$prob_under, 4),
             " | P(PUSH): ", round(res$prob_push, 4)), "\n",
      paste0("Edge Strength |Over-Under|: ", round(res$edge_strength, 4))
    )
  })

  output$prediction_table <- renderTable({
    if (!details_visible()) return(NULL)

    res <- model_results()
    if (is.character(res)) return(NULL)
    res$details
  }, striped = TRUE, bordered = TRUE, hover = TRUE)
}

shinyApp(ui = ui, server = server)
