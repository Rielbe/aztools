#' Download all files from a directory in an Azure Storage Account's container
#'
#' This function downloads all the files located at the given az_path recursively
#' and saves them in the directory specified by local_path.
#'
#' @param az_path String. Path to the directory that will be downloaded. If provided, it must end with "/".
#' @param local_path String. Local directory where the files will be stored.
#' @param account_name String. Storage account name.
#' @param container_name String. Container name.
#' @param sas_token String. SAS token with the required permissions to download the files.
#' @return The output of the download process.
#' @export
Download_Batch <- function(az_path = "", local_path = ".", account_name, container_name, sas_token) {
  download_query <- paste0("az storage copy -s ",
                           "'https://", account_name, ".blob.core.windows.net/",
                           container_name, "/", az_path, sas_token, "'",
                           " -d ", local_path, " --recursive")

  system(download_query, intern = T)
}

#' Upload all files from a local directory to an Azure Storage Account's container
#'
#' This function uploads all files located at the given local_path recursively
#' and saves them in the directory specified by az_path.
#'
#' @param az_path String. Path to the directory where files will be uploaded. If provided, it must end with "/".
#' @param local_path String. Local directory where the files to upload are located.
#' @param account_name String. Storage account name.
#' @param container_name String. Container name.
#' @param sas_token String. SAS token with the required permissions to upload the files.
#' @return The output of the upload process.
#' @export
Upload_Batch <- function(az_path = "", local_path = ".", account_name, container_name, sas_token) {
  upload_query <- paste0("az storage copy -d ",
                         "'https://", account_name, ".blob.core.windows.net/",
                         container_name, "/", az_path, sas_token, "'",
                         " -s ", local_path, " --recursive")

  system(upload_query, intern = T)
}
