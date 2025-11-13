using Microsoft.EntityFrameworkCore;
using ECommerceAnalytics.Api.Models;

namespace ECommerceAnalytics.Api.Data
{
    public class ApplicationDbContext : DbContext
    {
        public ApplicationDbContext(DbContextOptions<ApplicationDbContext> options)
            : base(options)
        {
        }

        // DbSet properties for entities
        public DbSet<Customer> Customers { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            // Customer configuration - map to dim_customer table
            modelBuilder.Entity<Customer>(entity =>
            {
                entity.ToTable("dim_customer");
                entity.HasKey(e => e.CustomerKey);
                entity.Property(e => e.CustomerKey).HasColumnName("customer_key");
                entity.Property(e => e.CustomerId).HasColumnName("customer_id").IsRequired().HasMaxLength(50);
                entity.Property(e => e.CustomerUniqueId).HasColumnName("customer_unique_id").IsRequired().HasMaxLength(50);
                entity.Property(e => e.CustomerName).HasColumnName("customer_name").HasMaxLength(100);
                entity.Property(e => e.CustomerPhone).HasColumnName("customer_phone").HasMaxLength(20);
                entity.Property(e => e.CustomerEmail).HasColumnName("customer_email").HasMaxLength(100);
                entity.Property(e => e.CustomerCity).HasColumnName("customer_city").HasMaxLength(100);
                entity.Property(e => e.CustomerState).HasColumnName("customer_state").HasMaxLength(10);
                entity.Property(e => e.CustomerRegion).HasColumnName("customer_region").HasMaxLength(50);
                entity.Property(e => e.CustomerTier).HasColumnName("customer_tier").HasMaxLength(20);
                entity.Property(e => e.IsActive).HasColumnName("is_active");
                entity.Property(e => e.CreatedDate).HasColumnName("created_date");
            });
        }

        // For raw SQL queries and views
        public IQueryable<CustomerAnalytics> CustomerAnalytics =>
            Set<CustomerAnalytics>().FromSqlRaw("SELECT * FROM v_customer_analytics");
    }
}