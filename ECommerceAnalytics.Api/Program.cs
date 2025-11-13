using Microsoft.EntityFrameworkCore;
using ECommerceAnalytics.Api.Data;
using ECommerceAnalytics.Api.Models;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// Configure Entity Framework
builder.Services.AddDbContext<ApplicationDbContext>(options =>
    options.UseSqlite("Data Source=../ecommerce_analytics.db"));

// Add CORS
builder.Services.AddCors(options =>
{
    options.AddPolicy("AllowAll", builder =>
    {
        builder.AllowAnyOrigin()
               .AllowAnyMethod()
               .AllowAnyHeader();
    });
});

var app = builder.Build();

// Configure the HTTP request pipeline
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseCors("AllowAll");
app.MapControllers();

// Add test endpoint for verification
app.MapGet("/", () => "E-Commerce Analytics API - Running!");
app.MapGet("/test", () => Results.Ok(new {
    message = "API is working!",
    time = DateTime.Now,
    status = "operational"
}));

// Database connection temporarily disabled for testing
Console.WriteLine("⚠️ Database connection disabled for testing");

app.Run();
