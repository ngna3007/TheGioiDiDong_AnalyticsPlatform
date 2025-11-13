namespace ECommerceAnalytics.Api.Models
{
    public class Customer
    {
        public int CustomerKey { get; set; }
        public string CustomerId { get; set; } = string.Empty;
        public string CustomerUniqueId { get; set; } = string.Empty;
        public string CustomerName { get; set; } = string.Empty;
        public string CustomerPhone { get; set; } = string.Empty;
        public string CustomerEmail { get; set; } = string.Empty;
        public string CustomerCity { get; set; } = string.Empty;
        public string CustomerState { get; set; } = string.Empty;
        public string CustomerRegion { get; set; } = string.Empty;
        public string CustomerTier { get; set; } = string.Empty;
        public bool IsActive { get; set; }
        public DateTime CreatedDate { get; set; }
    }

    public class CustomerAnalytics
    {
        public int CustomerKey { get; set; }
        public string CustomerId { get; set; } = string.Empty;
        public string CustomerCity { get; set; } = string.Empty;
        public string CustomerState { get; set; } = string.Empty;
        public string CustomerRegion { get; set; } = string.Empty;
        public string CustomerTier { get; set; } = string.Empty;
        public int TotalOrders { get; set; }
        public decimal TotalRevenue { get; set; }
        public decimal AvgOrderValue { get; set; }
    }
}