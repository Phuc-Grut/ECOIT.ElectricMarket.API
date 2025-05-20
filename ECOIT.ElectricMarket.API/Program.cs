using ECOIT.ElectricMarket.Aplication.Interface;
using ECOIT.ElectricMarket.Application.Interface;
using ECOIT.ElectricMarket.Application.Services;
using ECOIT.ElectricMarket.Infrastructure.SQL;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
//ExcelPackage.License = new OfficeOpenXml.License.NonCommercialLicense();

builder.Services.AddScoped<IDynamicTableService, DynamicTableService>();
builder.Services.AddScoped<ISheetImportHandler, SheetImportHandler>();
builder.Services.AddScoped<ICaculateServices, CaculateServices>();
builder.Services.AddScoped<ICalculateTableServices, CalculateTableServices>();
builder.Services.AddScoped<IFile4Services, File4Services>();
builder.Services.AddScoped<ICsport, CalculateCsportServices>();

// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services.AddCors(options =>
{
    options.AddPolicy("AllowReact", builder =>
    {
        builder.WithOrigins("http://localhost:3000")
               .AllowAnyHeader()
               .AllowAnyMethod();
    });
});

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}
app.UseCors("AllowReact");
app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();