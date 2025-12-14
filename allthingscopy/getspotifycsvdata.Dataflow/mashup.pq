[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[BindToDefaultDestination = true]
shared #"your_top_songs_2016 1" = let
    // 1️⃣ SharePoint personal site root
    SiteURL = "https://eduzseni-my.sharepoint.com/personal/roberto_rober_to",

    // 2️⃣ Get all files from the site
    Source = SharePoint.Files(SiteURL, [ApiVersion = 15]),

    // 3️⃣ Filter files inside /Documents/spotifycsv/
    SpotifyFolder =
        Table.SelectRows(
            Source,
            each Text.StartsWith([Folder Path], SiteURL & "/Documents/spotifycsv/")
        ),

    // 4️⃣ Keep only CSV files
    CSVFiles = Table.SelectRows(SpotifyFolder, each Text.EndsWith([Name], ".csv")),

    // 5️⃣ Load CSV contents
    Imported =
        Table.AddColumn(
            CSVFiles,
            "Data",
            each Csv.Document(
                [Content],
                [
                    Delimiter = ",",
                    Encoding = 65001,
                    QuoteStyle = QuoteStyle.None
                ]
            )
        ),

    // 6️⃣ Promote headers
    Promoted =
        Table.TransformColumns(
            Imported,
            {"Data", each Table.PromoteHeaders(_, [PromoteAllScalars = true])}
        ),

    // 7️⃣ snake_case function
    ToSnakeCase = (name as text) as text =>
        let
            clean = Text.Remove(name, {"(", ")", "[", "]"}),
            r1 = Text.Replace(clean, " ", "_"),
            r2 = Text.Replace(r1, "-", "_"),
            collapse = List.Accumulate({"___","__"}, r2, (state, x) => Text.Replace(state, x, "_")),
            trimmed = Text.Trim(collapse, "_"),
            lower = Text.Lower(trimmed)
        in
            lower,

    // 7b️⃣ Apply snake_case to all files
    CleanedColumns =
        Table.TransformColumns(
            Promoted,
            {"Data", each Table.TransformColumnNames(_, ToSnakeCase)}
        ),

    // 8️⃣ Add ytyear + source_file
    AddedYearColumn =
        Table.TransformRows(
            CleanedColumns,
            each
                let
                    FileName = [Name],
                    digitsPos = Text.PositionOfAny(FileName, {"0".."9"}),
                    YearText =
                        if digitsPos >= 0 and Text.Length(FileName) >= digitsPos + 4
                        then Text.Middle(FileName, digitsPos, 4)
                        else null,

                    YearNumber = try Number.FromText(YearText) otherwise null,

                    WithYear =
                        Table.AddColumn(
                            [Data],
                            "ytyear",
                            each YearNumber,
                            Int64.Type
                        ),

                    WithSource =
                        Table.AddColumn(
                            WithYear,
                            "source_file",
                            each FileName,
                            type text
                        )
                in
                    [
                        Data = WithSource,
                        Name = FileName
                    ]
        ),

    // 9️⃣ Combine all CSVs
    Combined = Table.Combine(List.Transform(AddedYearColumn, each _[Data])),

    // 🔟 FIX COLUMN TYPES (manual — correct & reliable)
    FinalTable =
        Table.TransformColumnTypes(Combined, {{"duration_ms", Int64.Type}, {"popularity", Int64.Type}, {"danceability", type number}, {"energy", type number}, {"loudness", type number}, {"speechiness", type number}, {"acousticness", type number}, {"instrumentalness", type number}, {"liveness", type number}, {"valence", type number}, {"tempo", type number}, {"time_signature", Int64.Type}, {"explicit", type logical}, {"ytyear", Int64.Type}, {"release_date", type datetime}, {"added_at", type datetime}}, "en-US")
in
    FinalTable;
shared DefaultDestination = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  #"Navigation 1" = Source{[workspaceId = "4c071025-89dc-49cc-8031-06c564ed2f2d"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "ad21b38c-85f1-42b8-ad11-db43446523be"]}[Data]
in
  #"Navigation 2";
