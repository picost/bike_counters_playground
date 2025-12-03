import logging
import re
import json
import requests
from typing import Dict, Optional
import pandas as pd
from bs4 import BeautifulSoup

class EcoCounterScraper(object):
    """A simple scraper for Eco-Counter display map data.
    
    This class provides methods to fetch and parse bicycle count data
    from the Eco-Counter display map website without using a browser.
    
    Note: This scraper relies on the current structure of the Eco-Counter
    display map pages and may break if the website changes.
    """
    base_url = "https://eco-display-map.eco-counter.com"
    freq_to_granularity_api = {
        "D": "P1D",    # Day
        "W": "P1W",    # Week  
        "M": "P1M",    # Month
        "Y": "P1Y",    # Year
    }

    def __init__(self, site_id, debug=False):
        """Initialize the scraper with an optional site identifier.

        Parameters
        ----------
        site_id: int or str
            Eco-Counter site identifier.
            E.g. "300037212" for Cagnes sur Mer (FR)
        """
        self._site_id = site_id
        # site_id_cagnes_littoral = "300037212"  # Cagnes sur Mer (FR)
        self.logger = logging.getLogger(f"{self.__class__.__name__}.site_{site_id}")
        self.is_initialized = False
        self.debug = debug

    def __repr__(self):
        s_repr= (
            f"{self.__class__.__name__}(site_id={self.site_id})"
        )
        if self.is_initialized:
            s_repr += f"\n  Site name: {self.site_name_}"
            s_repr += f"\n  Location: {self.site_location_}"
            s_repr += f"\n  First data date: {self.site_first_data_.date()}"
        else:
            s_repr += "\n  [Not yet initialized]"
        return s_repr

    @property
    def site_id(self):
        """Get the site identifier."""
        return self._site_id


    #----- Public Methods ---------------------------------
    def fetch_counts(
        self,
        start=None,
        end=None,
        freq='D',
        ):
        """Fetch count data from Eco-Counter display map.
        
        Parameters
        ---------
        start: pd.Timestamp or alike, optional
            Beginning (included) of the requested period.
            If None, one period before end is fetched.
        end: pd.Timestamp or alike, optional
            End of the requested period (included). If None, today is used.
        freq: str, default='D'
            Frequency/granularity ('D'=daily, 'W'=weekly, 'M'=monthly, 'Y'=yearly

        Returns
        -------
        pd.DataFrame :
            DataFrame containing the count data with a DateTimeIndex and
            three columns: 'count', 'in', 'out'.
        
        Raises
        ------
        ValueError:
            If site_id is not provided either in argument or at initialization.
        ValueError: 
            If dates are invalid or frequency not supported
        requests.RequestException: If HTTP request fails
        """
        end = pd.Timestamp(end) if end is not None else pd.Timestamp.now(tz='Europe/Paris')
        start = pd.Timestamp(start) if start is not None else end - pd.tseries.frequencies.to_offset(freq)
        # Scrape the data
        json_like_data = self._scrape_count_structure(
            self.site_id,
            start,
            end,
            freq,
        )
        # Extract and combine global and directional counts
        data = (self._extract_global_counts(json_like_data)
                    .join(self._extract_directional_counts(json_like_data), how='outer'))
        return data

    #========================================================
    def _scrape_count_structure(
        self,
        site_id,
        start,
        end,
        freq="D",
    ):
        """Return dict with count data from Eco-Counter display map.
        
        Parameters
        ---------
        site_id: int or str
            Eco-Counter site identifier.
            E.g. "300037212" for Cagnes sur Mer (FR)
        start: pd.Timestamp or alike
            Beginning (included) of the requested period.
        end: pd.Timestamp or alike
            End of the requested period (included).
        freq: 
            Frequency/granularity ('D'=daily, 'W'=weekly, 'M'=monthly, 'Y'=yearly)
            
        Returns
        -------
        dict :
            Dictionary containing metadata, KPIs, and time series data.
            
        Raises
        ------
        ValueError: If dates are invalid or frequency not supported
        requests.RequestException: If HTTP request fails

        Notes
        -----
        Scrape count data using simple HTTP requests (no browser needed!).
        
        This function:
        1. Builds the URL with the correct parameters
        2. Fetches the HTML with a simple HTTP GET
        3. Extracts the embedded JSON data using regex
        4. Returns structured data as a dictionary.

        
        .. note::
            If ``self.debug`` is True, the fetched HTML content is stored
            in the attribute ``self.fetched_html_`` for debugging purposes.
        
        """
        # Validate dates
        start_date = pd.Timestamp(start)
        end_date = pd.Timestamp(end)
        if start_date >= end_date:
            raise ValueError(f"Start date ({start}) must be before end date ({end})")
        try:
            granularity = self.freq_to_granularity_api[freq]
        except KeyError:
            raise ValueError(f"Unsupported frequency '{freq}'. Use one of {list(self.freq_to_granularity_api.keys())}")
        # Build URL
        url = self._build_url(
            site_id, 
            granularity=granularity,
            start=start_date.date().isoformat(),
            end=end_date.date().isoformat(),
            )
        self.logger.debug(f"Fetching data from: {url}")
        # Fetch HTML
        html_data = self._fetch_html(url)
        if self.debug:
            self.fetched_html_ = html_data
        self.logger.debug(f"Downloaded {len(html_data)} bytes")
        self.logger.debug("Extracting data...")
        result = self._extract_nextjs_data(html_data)
        if not self.is_initialized:
            self._site_metadata(html_data)
            self._set_direction_names(result)
            self.is_initialized = True
        return result

    def _build_url(
        self, 
        site_id,
        granularity='P1W',
        start=None,
        end=None,
        ):
        """Build the URL to obtain the html page where to scrap data.
        
        Parameters
        ----------
        site_id: str or int
            The counting site identifier
        granularity: {"P1D", "P1W", "P1M", "P1Y"}
            ISO 8601 duration (P1D, P1W, P1M, P1Y).
            Default is "P1W" (weekly).
        start: str, optional
            Start date in 'YYYY-MM-DD' format
        end: str, optional
            End date in 'YYYY-MM-DD' format
            
        Returns
        -------
        str : 
            Complete URL to fetch data from.
            
        Examples
        --------
        >>> build_url("300037212", "P1Y")
        'https://eco-display-map.eco-counter.com/site/300037212?granularity=P1Y'
        """
        url = f"{self.base_url}/site/{site_id}?granularity={granularity}"
        if start:
            url += f"&startDate={start}"
        if end:
            url += f"&endDate={end}"
        return url

    def _fetch_html(self, url: str) -> str:
        """Return the HTML content fetched from a URL.
        
        Parameters
        ----------
        url: The URL to fetch
            
        Returns
        -------
        str: HTML content as string
            
        Raises
        ------
        requests.RequestException: If the request fails
            
        Examples
        --------
        >>> html = fetch_html("https://eco-display-map.eco-counter.com/site/300037212")
        >>> print(len(html))
        500000  # approximate

        """
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()  # Raise error if request failed
        return response.text

    def _extract_nextjs_data(self, html_data: str) -> Optional[Dict]:
        """Return the json-like part of the page containing the data of interest as a dict.
        
        Parameters
        ----------
        html_data : str
            The HTML content fetched from the Eco-Counter display map site.
            
        Returns
        -------
        dict or None
            The extracted data dictionary.

        Notes
        -----
        This function was written specifically to parse the Eco-Counter
        display map pages, which embed data in a specific JavaScript format.
        It may break at any time if the website structure changes.

        """
        self.logger.debug("Parsing HTML to extract embedded count data...")
        soup = BeautifulSoup(html_data, 'html.parser')
        # Find all script tags
        for script in soup.find_all('script'):
            script_content = script.string
            if not script_content:
                continue
            # Look for the __next_f.push pattern
            if 'self.__next_f.push' in script_content and 'chartData' in script_content:
                # Extract the data between the push() call
                # Pattern: self.__next_f.push([1,"..."])
                match = re.search(r'self\.__next_f\.push\(\[1,"(.+?)"\]\)', script_content, re.DOTALL)
                if match:
                    # Get the escaped JSON string
                    escaped_json = match.group(1)
                    # Unescape it
                    # Replace \\" with " and \\\\ with \\
                    unescaped = escaped_json.replace('\\\\', '\x00')  # Temporary placeholder
                    unescaped = unescaped.replace('\\"', '"')
                    unescaped = unescaped.replace('\x00', '\\')  # Restore backslashes
                    # Find the JSON object containing chartData
                    # Look for the pattern: {"params":... ,"chartData":[...], ...}
                    # We need to extract from the first { to its matching }
                    # Find where chartData object starts
                    chart_start = unescaped.find('"chartData"')
                    if chart_start == -1:
                        continue
                    # Go backwards to find the opening brace of the parent object
                    brace_pos = chart_start
                    while brace_pos > 0:
                        if unescaped[brace_pos] == '{':
                            # Check if this is the right level
                            # by counting if it contains the key structure we expect
                            break
                        brace_pos -= 1
                    # Now find the matching closing brace
                    brace_count = 0
                    start_pos = brace_pos
                    end_pos = start_pos
                    for i in range(start_pos, len(unescaped)):
                        if unescaped[i] == '{':
                            brace_count += 1
                        elif unescaped[i] == '}':
                            brace_count -= 1
                            if brace_count == 0:
                                end_pos = i + 1
                                break
                    # Extract the JSON substring
                    json_str = unescaped[start_pos:end_pos]
                    try:
                        # Parse with json.loads
                        data = json.loads(json_str)
                        # Verify we got the right data
                        if 'chartData' in data and 'kpi' in data:
                            return data
                    except json.JSONDecodeError as e:
                        # If json.loads fails, the structure might have some issues
                        # Print debug info
                        self.logger.debug(f"JSON decode error: {e}")
                        self.logger.debug(f"Problematic JSON substring (first 200 chars): {json_str[:200]}")
                        continue
        return {}

    def _extract_global_counts(self, fetched_data):
        """Return the traffic data for each period in a DataFrame

        Parameters
        ----------
        fetched_data : dict
            The data dictionary returned by scrape_counts()
        
        Returns
        -------
        pd.DataFrame
            The count of bicycles per period, without distinguishing direction.

        Notes
        -----
        The returned DataFrame has a DateTimeIndex and a single column 'count'.
        The data is fetched from the 'chartData' field in the fetched_data.
        """
        self.logger.debug("Transforming global counts to DataFrame.")
        chart_data = fetched_data['chartData'][0]['data']
        data_table = self._counts_as_df(chart_data)
        return data_table

    def _counts_as_df(self, data_field):
        """Return a dataframf from the data field of counts.

        Parameters
        ----------
        data_field : list of dict
            The 'data' field from either global or directional counts.
            The data is a list of dicts with 'timestamp' and 'traffic' keys,
            where the counts are under traffic['counts'].

        Returns
        -------
        pd.DataFrame
            DataFrame with DateTimeIndex and a single 'count' column.
        """
        data_table = pd.DataFrame(
            [(pd.Timestamp(entry['timestamp']), entry['traffic']['counts']) for entry in data_field],
            columns=['timestamp', 'count'],
        )
        return data_table.set_index('timestamp')

    def _extract_directional_counts(self, fetched_data):
        """Return the directional traffic data for each period in a DataFrame

        Parameters
        ----------
        fetched_data : dict
            The data dictionary returned by scrape_counts()
        
        Returns
        -------
        pd.DataFrame
            The count of bicycles per period, with separate columns for each direction.

        Notes
        -----
        The returned DataFrame has a DateTimeIndex and two columns: 'in' and 'out'.
        The data is fetched from the 'directionGraphData' field in the fetched_data.
        """
        self.logger.debug("Transforming directional counts to DataFrame.")
        dir_data = []
        for direction_data in fetched_data['directionGraphData']:
            dir_name = direction_data['direction']
            dir_data.append(self._counts_as_df(direction_data['data']))
            dir_data[-1].rename(columns={'count': dir_name}, inplace=True)
        # join on timestamp
        data_table = dir_data[0]
        for df in dir_data[1:]:
            data_table = data_table.join(df, how='outer')
        return data_table
    
    def _site_metadata(self, html_data):
        """Return site metadata from HTML content and set associated attributes.

        Parameters
        ----------
        html_data : str
            The HTML content fetched from the Eco-Counter display map site.

        Returns
        -------
        dict
            Dictionary containing site metadata such as site_id, site_name,
            location (lat, lon), and first_data date.

        """
        meta_data_pattern = (r'\\"currentSite\\":{\\"id\\":(?P<site_id>[\d]+),'
                             r'\\"name\\":\\"(?P<site_name>.*?)\\",'
                             r'\\"location\\":{\\"lat\\":(?P<lat>[-\d.]+),'
                             r'\\"lon\\":(?P<lon>[-\d.]+)},'
                             r'\\"firstData\\":\\"(?P<first_data>.*?)\\",')
        site_data = re.search(meta_data_pattern, html_data).groupdict()
        self.site_name_ = site_data['site_name']
        self.site_location_ = {
            'lat': float(site_data['lat']),
            'lon': float(site_data['lon']),
        }
        self.site_first_data_ = pd.Timestamp(site_data['first_data'])
        return site_data
        
    def _set_direction_names(self, fetched_data):
        """Return dict matching direction codes to their names.

        Sets the `direction_names_` attribute as a side effect.

        Parameters
        ----------
        fetched_data : dict
            The data dictionary returned by scrape_counts()
        
        Returns
        -------
        dict
            Dictionary mapping direction codes (in, out) to human-readable names.

        """        
        direction_names = {}
        for direction_data in fetched_data['directionGraphData']:
            dir_name = direction_data['direction']
            direction_names[dir_name] = direction_data['directionName']
        self.direction_names_ = direction_names
        return direction_names
