import re
from typing import List
import logging
import pandas as pd

logger = logging.getLogger(__name__)

# Try to import pycountry and langcodes, fallback to basic mapping if not available
try:
    import pycountry
    PYCOUNTRY_AVAILABLE = True
except ImportError:
    PYCOUNTRY_AVAILABLE = False
    logger.warning("pycountry not available, using fallback country mapping")

try:
    import langcodes
    LANGCODES_AVAILABLE = True
except ImportError:
    LANGCODES_AVAILABLE = False
    logger.warning("langcodes not available, using fallback language mapping")

class ISOMapper:
    """Utility class for mapping ISO codes to readable names."""
    
    @staticmethod
    def get_country_name(iso_code: str) -> str:
        """Convert ISO country code to country name."""
        if not iso_code or pd.isna(iso_code):
            return ""
        
        iso_code = str(iso_code).strip().upper()
        
        if PYCOUNTRY_AVAILABLE:
            try:
                country = pycountry.countries.get(alpha_2=iso_code)
                if country:
                    return country.name
                
                # Try alpha_3 if alpha_2 fails
                country = pycountry.countries.get(alpha_3=iso_code)
                if country:
                    return country.name
            except Exception as e:
                logger.debug(f"pycountry lookup failed for {iso_code}: {e}")
        
        # Fallback mapping for common codes
        fallback_countries = {
            'US': 'United States', 'GB': 'United Kingdom', 'FR': 'France',
            'DE': 'Germany', 'IT': 'Italy', 'JP': 'Japan', 'CA': 'Canada',
            'AU': 'Australia', 'ES': 'Spain', 'IN': 'India', 'CN': 'China',
            'RU': 'Russia', 'BR': 'Brazil', 'MX': 'Mexico', 'KR': 'South Korea',
            'NL': 'Netherlands', 'SE': 'Sweden', 'NO': 'Norway', 'DK': 'Denmark'
        }
        
        return fallback_countries.get(iso_code, iso_code)
    
    @staticmethod
    def get_language_name(iso_code: str) -> str:
        """Convert ISO language code to language name."""
        if not iso_code or pd.isna(iso_code):
            return ""
        
        iso_code = str(iso_code).strip().lower()
        
        if LANGCODES_AVAILABLE:
            try:
                language = langcodes.Language.make(language=iso_code)
                return language.display_name()
            except Exception as e:
                logger.debug(f"langcodes lookup failed for {iso_code}: {e}")
        
        # Fallback mapping for common codes
        fallback_languages = {
            'en': 'English', 'fr': 'French', 'de': 'German', 'es': 'Spanish',
            'it': 'Italian', 'ja': 'Japanese', 'ko': 'Korean', 'zh': 'Chinese',
            'ru': 'Russian', 'pt': 'Portuguese', 'nl': 'Dutch', 'sv': 'Swedish',
            'da': 'Danish', 'no': 'Norwegian', 'fi': 'Finnish', 'pl': 'Polish',
            'ar': 'Arabic', 'hi': 'Hindi', 'th': 'Thai', 'vi': 'Vietnamese'
        }
        
        return fallback_languages.get(iso_code, iso_code)
    
    @staticmethod
    def clean_and_map_countries(data_str: str) -> List[str]:
        """Parse country data - handles both JSON format with ISO codes and plain text."""
        if pd.isna(data_str) or data_str is None or str(data_str).strip() == "":
            return []
        
        try:
            data_str = str(data_str).strip()
            
            # Check if it's JSON-like format (contains quotes and colons)
            if ("'" in data_str or '"' in data_str) and ":" in data_str:
                # Handle JSON-like format with potential ISO codes
                return ISOMapper._parse_json_countries(data_str)
            else:
                # Handle plain text format - already in correct format, just clean and split
                return ISOMapper._parse_plain_text_countries(data_str)
            
        except Exception as e:
            logger.warning(f"Could not parse countries data {data_str}: {e}")
            return []
    
    @staticmethod
    def _parse_json_countries(json_str: str) -> List[str]:
        """Parse JSON-like country data with ISO code mapping."""
        # Extract both 'name' and 'iso_3166_1' patterns
        name_pattern = r"'name':\s*'([^']+)'|\"name\":\s*\"([^\"]+)\""
        iso_pattern = r"'iso_3166_1':\s*'([^']+)'|\"iso_3166_1\":\s*\"([^\"]+)\""
        
        # Get names first
        name_matches = re.findall(name_pattern, json_str)
        names = [match[0] if match[0] else match[1] for match in name_matches if any(match)]
        
        # Get ISO codes and convert them
        iso_matches = re.findall(iso_pattern, json_str)
        iso_codes = [match[0] if match[0] else match[1] for match in iso_matches if any(match)]
        
        # Convert ISO codes to names
        mapped_countries = []
        for code in iso_codes:
            if code:
                mapped_name = ISOMapper.get_country_name(code)
                if mapped_name and mapped_name != code:
                    mapped_countries.append(mapped_name)
        
        # Combine and deduplicate, preferring mapped names over original names
        all_countries = mapped_countries + names
        return list(dict.fromkeys(all_countries))  # Remove duplicates while preserving order
    
    @staticmethod
    def _parse_plain_text_countries(text_str: str) -> List[str]:
        """Parse plain text country data that's already in readable format."""
        # Split by common delimiters and clean
        delimiters = [',', ';', '|', ' and ', ' & ']
        
        # Try each delimiter
        for delimiter in delimiters:
            if delimiter in text_str:
                countries = [country.strip() for country in text_str.split(delimiter)]
                # Filter out empty strings and return non-empty countries
                return [country for country in countries if country and len(country) > 1]
        
        # If no delimiter found, treat as single country
        cleaned_country = text_str.strip()
        return [cleaned_country] if cleaned_country and len(cleaned_country) > 1 else []
    
    @staticmethod
    def clean_and_map_languages(data_str: str) -> List[str]:
        """Parse language data - handles both JSON format with ISO codes and plain text."""
        if pd.isna(data_str) or data_str is None or str(data_str).strip() == "":
            return []
        
        try:
            data_str = str(data_str).strip()
            
            # Check if it's JSON-like format (contains quotes and colons)
            if ("'" in data_str or '"' in data_str) and ":" in data_str:
                # Handle JSON-like format with potential ISO codes
                return ISOMapper._parse_json_languages(data_str)
            else:
                # Handle plain text format - already in correct format, just clean and split
                return ISOMapper._parse_plain_text_languages(data_str)
            
        except Exception as e:
            logger.warning(f"Could not parse languages data {data_str}: {e}")
            return []
    
    @staticmethod
    def _parse_json_languages(json_str: str) -> List[str]:
        """Parse JSON-like language data with ISO code mapping."""
        # Extract both 'name' and 'iso_639_1' patterns
        name_pattern = r"'name':\s*'([^']+)'|\"name\":\s*\"([^\"]+)\""
        iso_pattern = r"'iso_639_1':\s*'([^']+)'|\"iso_639_1\":\s*\"([^\"]+)\""
        
        # Get names first
        name_matches = re.findall(name_pattern, json_str)
        names = [match[0] if match[0] else match[1] for match in name_matches if any(match)]
        
        # Get ISO codes and convert them
        iso_matches = re.findall(iso_pattern, json_str)
        iso_codes = [match[0] if match[0] else match[1] for match in iso_matches if any(match)]
        
        # Convert ISO codes to names
        mapped_languages = []
        for code in iso_codes:
            if code:
                mapped_name = ISOMapper.get_language_name(code)
                if mapped_name and mapped_name != code:
                    mapped_languages.append(mapped_name)
        
        # Combine and deduplicate, preferring mapped names over original names
        all_languages = mapped_languages + names
        return list(dict.fromkeys(all_languages))  # Remove duplicates while preserving order
    
    @staticmethod
    def _parse_plain_text_languages(text_str: str) -> List[str]:
        """Parse plain text language data that's already in readable format."""
        # Split by common delimiters and clean
        delimiters = [',', ';', '|', ' and ', ' & ']
        
        # Try each delimiter
        for delimiter in delimiters:
            if delimiter in text_str:
                languages = [lang.strip() for lang in text_str.split(delimiter)]
                # Filter out empty strings and return non-empty languages
                return [lang for lang in languages if lang and len(lang) > 1]
        
        # If no delimiter found, treat as single language
        cleaned_language = text_str.strip()
        return [cleaned_language] if cleaned_language and len(cleaned_language) > 1 else []
