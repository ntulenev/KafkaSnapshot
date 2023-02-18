namespace KafkaSnapshot.Models.Names
{
    public class FileName
    {
        public string FullName => _fullName;
        public string Extension => _extension;

        public FileName(string fileName)
        {
            ArgumentNullException.ThrowIfNull(fileName);

            if (string.IsNullOrWhiteSpace(fileName))
            {
                throw new ArgumentException("File name cannot be null, empty or whitespace.", nameof(fileName));
            }

            _fullName = fileName;
            _extension = Path.GetExtension(fileName);
        }

        private readonly string _fullName;
        private readonly string _extension;
    }
}
