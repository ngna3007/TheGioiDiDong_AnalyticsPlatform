using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using ECommerceAnalytics.Api.Data;
using ECommerceAnalytics.Api.Models;

namespace ECommerceAnalytics.Api.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class CustomersController : ControllerBase
    {
        private readonly ApplicationDbContext _context;

        public CustomersController(ApplicationDbContext context)
        {
            _context = context;
        }

        [HttpGet("test")]
        public ActionResult<TestResponse> GetTest()
        {
            return Ok(new TestResponse
            {
                Message = "API is working!",
                Timestamp = DateTime.UtcNow,
                Status = "healthy"
            });
        }

        [HttpGet]
        public async Task<ActionResult<IEnumerable<Customer>>> GetCustomers(
            [FromQuery] int page = 1,
            [FromQuery] int pageSize = 50)
        {
            try
            {
                var customers = await _context.Customers
                    .Skip((page - 1) * pageSize)
                    .Take(pageSize)
                    .ToListAsync();

                return Ok(customers);
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { error = "Failed to retrieve customers", message = ex.Message });
            }
        }

        [HttpGet("{id}")]
        public async Task<ActionResult<Customer>> GetCustomer(string id)
        {
            try
            {
                var customer = await _context.Customers
                    .FirstOrDefaultAsync(c => c.CustomerId == id);

                if (customer == null)
                {
                    return NotFound();
                }

                return Ok(customer);
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { error = "Failed to retrieve customer", message = ex.Message });
            }
        }

        [HttpGet("summary")]
        public async Task<ActionResult<object>> GetCustomerSummary()
        {
            try
            {
                var totalCustomers = await _context.Customers.CountAsync();
                var activeCustomers = await _context.Customers.CountAsync(c => c.IsActive);

                return Ok(new
                {
                    totalCustomers,
                    activeCustomers,
                    lastUpdated = DateTime.UtcNow,
                    apiStatus = "healthy"
                });
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { error = "Failed to retrieve customer summary", message = ex.Message });
            }
        }

        [HttpGet("top")]
        public async Task<ActionResult<IEnumerable<Customer>>> GetTopCustomers(
            [FromQuery] int count = 10)
        {
            try
            {
                var customers = await _context.Customers
                    .Take(count)
                    .ToListAsync();

                return Ok(customers);
            }
            catch (Exception ex)
            {
                return StatusCode(500, new { error = "Failed to retrieve top customers", message = ex.Message });
            }
        }
    }

    public class TestResponse
    {
        public string Message { get; set; } = string.Empty;
        public DateTime Timestamp { get; set; }
        public string Status { get; set; } = string.Empty;
    }
}