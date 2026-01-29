"""
Test script for anti-rate-limiting features.
"""
import unittest
import os
from scripts.full_gumroad_scrape import AdaptiveDelayConfig
from gumroad_scraper import get_random_user_agent, get_proxy_config


class TestUserAgentRotation(unittest.TestCase):
    """Test user agent rotation."""
    
    def test_user_agent_rotation(self):
        """Test that user agent rotation works."""
        user_agents = set()
        for _ in range(10):
            ua = get_random_user_agent()
            user_agents.add(ua)
            self.assertIsInstance(ua, str)
            self.assertGreater(len(ua), 0)
            self.assertIn("Mozilla", ua)
        
        # Should get at least 2 different user agents in 10 tries
        self.assertGreaterEqual(len(user_agents), 2, "User agent rotation not working")


class TestProxyConfig(unittest.TestCase):
    """Test proxy configuration."""
    
    def tearDown(self):
        """Clean up env vars after each test."""
        for key in ["SCRAPER_PROXY_URL", "SCRAPER_PROXY_USER", "SCRAPER_PROXY_PASS"]:
            if key in os.environ:
                del os.environ[key]
    
    def test_proxy_config_no_env(self):
        """Test proxy config when no env vars are set."""
        config = get_proxy_config()
        self.assertIsNone(config, "Proxy config should be None when no env vars set")
    
    def test_proxy_config_with_url(self):
        """Test proxy config with just URL."""
        os.environ["SCRAPER_PROXY_URL"] = "http://proxy.example.com:8080"
        
        config = get_proxy_config()
        self.assertIsNotNone(config)
        self.assertEqual(config["server"], "http://proxy.example.com:8080")
        self.assertNotIn("username", config)
        self.assertNotIn("password", config)
    
    def test_proxy_config_with_credentials(self):
        """Test proxy config with URL and credentials."""
        os.environ["SCRAPER_PROXY_URL"] = "http://proxy.example.com:8080"
        os.environ["SCRAPER_PROXY_USER"] = "testuser"
        os.environ["SCRAPER_PROXY_PASS"] = "testpass"
        
        config = get_proxy_config()
        self.assertIsNotNone(config)
        self.assertEqual(config["server"], "http://proxy.example.com:8080")
        self.assertEqual(config["username"], "testuser")
        self.assertEqual(config["password"], "testpass")


class TestAdaptiveDelayConfig(unittest.TestCase):
    """Test adaptive delay configuration."""
    
    def test_initial_values(self):
        """Test initial adaptive delay config values."""
        config = AdaptiveDelayConfig()
        
        self.assertEqual(config.consecutive_failures, 0)
        self.assertEqual(config.multiplier, 1.0)
        self.assertEqual(config.get_category_delay(), 60)
        self.assertEqual(config.get_subcategory_delay(), 30)
        self.assertEqual(config.get_failure_cooldown(), 300)
    
    def test_delay_increases_after_failure(self):
        """Test that delays increase after failure."""
        config = AdaptiveDelayConfig()
        
        config.record_failure()
        self.assertEqual(config.consecutive_failures, 1)
        self.assertEqual(config.multiplier, 1.5)
        self.assertEqual(config.get_category_delay(), 90)
        self.assertEqual(config.get_subcategory_delay(), 45)
        self.assertEqual(config.get_failure_cooldown(), 450)
    
    def test_delay_continues_increasing(self):
        """Test that delays continue to increase with more failures."""
        config = AdaptiveDelayConfig()
        
        config.record_failure()
        config.record_failure()
        config.record_failure()
        self.assertEqual(config.consecutive_failures, 3)
        self.assertEqual(config.multiplier, 2.5)
    
    def test_max_multiplier_cap(self):
        """Test that multiplier caps at max_multiplier."""
        config = AdaptiveDelayConfig()
        
        for _ in range(20):
            config.record_failure()
        self.assertEqual(config.multiplier, 4.0, "Multiplier should cap at max_multiplier")
    
    def test_success_reduces_failures(self):
        """Test that success reduces failure count."""
        config = AdaptiveDelayConfig()
        
        for _ in range(5):
            config.record_failure()
        failures_before = config.consecutive_failures
        
        config.record_success()
        self.assertLess(config.consecutive_failures, failures_before)
    
    def test_failure_count_floor(self):
        """Test that failure count doesn't go negative."""
        config = AdaptiveDelayConfig()
        
        for _ in range(20):
            config.record_success()
        self.assertEqual(config.consecutive_failures, 0)


if __name__ == "__main__":
    unittest.main()
