"""
VTM Log Parser
==============
Parses the output log of the encoder to extract metrics like Time, PSNR, SSIM, Bitrate, etc.
"""

import re
import os

def parse_vtm_log(log_text: str, output_bin: str) -> dict:
    metrics = {
        "time": "-",
        "psnr_y": "-",
        "psnr_u": "-",
        "psnr_v": "-",
        "psnr_yuv": "-",
        "bitrate": "-",
        "ssim": "-",
        "entropy": "-",
        "size": "-"
    }
    
    # Example VTM summary lines:
    #   Total Time:      123.456 sec.
    #   Total Time: 10.123 sec. F.R.: 2.055 Hz
    match_time = re.search(r"Total Time:\s*([0-9.]+)\s*sec", log_text)
    if match_time:
        metrics["time"] = f"{match_time.group(1)} s"
    
    # Summary line with Y-PSNR, U-PSNR, V-PSNR, and Bitrate often starts with something like:
    # "       a    " (summary line)
    # usually there's a row like:
    #        Y-PSNR    U-PSNR    V-PSNR    Y-UV-PSNR  Bitrate
    #   ...  38.1234   39.4567   40.1234   39.0000    1000.5678
    
    # We can just look for the summary line.
    # The summary block starts after "SUMMARY --------------------------------------------------------"
    # Then there's a line with the averages. The typical format for VTM:
    # \t Total Frames |   "Bitrate Y-PSNR U-PSNR V-PSNR YUV-PSNR" - VTM is typically:
    #   100    a     0.0123   36.0011   37.0011   38.0011   36.5011
    # Actually, a regex checking "a\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)" could match Bitrate and PSNRs.
    
    # Let's search for PSNRs and Bitrate directly by matching the typical float layout near the end.
    # A generic way if VTM is: 
    #   Bitrate: 1000.00 kbps
    # If standard VTM summary table:
    #  [0-9]+  a\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)
    # Usually: Bitrate Y-PSNR U-PSNR V-PSNR
    match_summary = re.search(
        r"\b(?:a|I)\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)(?:\s+([0-9.]+))?",
        log_text,
    )
    if match_summary:
        metrics["bitrate"] = match_summary.group(1) + " kbps"
        metrics["psnr_y"] = match_summary.group(2) + " dB"
        metrics["psnr_u"] = match_summary.group(3) + " dB"
        metrics["psnr_v"] = match_summary.group(4) + " dB"
        if match_summary.group(5):
            metrics["psnr_yuv"] = match_summary.group(5) + " dB"
        
    # Another approach, VTM prints:
    match_psnry = re.search(r"Y-PSNR\s*[:=]?\s*([0-9.]+)|PSNR-Y\s*[:=]?\s*([0-9.]+)", log_text, re.IGNORECASE)
    if match_psnry:
        metrics["psnr_y"] = (match_psnry.group(1) or match_psnry.group(2)) + " dB"

    match_psnru = re.search(r"U-PSNR\s*[:=]?\s*([0-9.]+)|PSNR-U\s*[:=]?\s*([0-9.]+)", log_text, re.IGNORECASE)
    if match_psnru:
        metrics["psnr_u"] = (match_psnru.group(1) or match_psnru.group(2)) + " dB"

    match_psnrv = re.search(r"V-PSNR\s*[:=]?\s*([0-9.]+)|PSNR-V\s*[:=]?\s*([0-9.]+)", log_text, re.IGNORECASE)
    if match_psnrv:
        metrics["psnr_v"] = (match_psnrv.group(1) or match_psnrv.group(2)) + " dB"

    match_psnryuv = re.search(
        r"(?:Y-UV-PSNR|YUV-PSNR|PSNR-YUV)\s*[:=]?\s*([0-9.]+)",
        log_text,
        re.IGNORECASE,
    )
    if match_psnryuv:
        metrics["psnr_yuv"] = match_psnryuv.group(1) + " dB"

    match_bitrate = re.search(r"Bitrate\s*[:=]?\s*([0-9.]+)", log_text, re.IGNORECASE)
    if match_bitrate:
        metrics["bitrate"] = match_bitrate.group(1) + " kbps"

    # Also check the typical VTM table format:
    # \s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)
    table_match = re.finditer(
        r"^\s*\d+\s+a\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)(?:\s+([0-9.]+))?",
        log_text,
        re.MULTILINE,
    )
    for tm in table_match:
        metrics["bitrate"] = tm.group(1) + " kbps"
        metrics["psnr_y"] = tm.group(2) + " dB"
        metrics["psnr_u"] = tm.group(3) + " dB"
        metrics["psnr_v"] = tm.group(4) + " dB"
        if tm.group(5):
            metrics["psnr_yuv"] = tm.group(5) + " dB"

    # SSIM
    match_ssim = re.search(r"SSIM\s*[:=]?\s*([0-9.]+)|SSIM(?:-Y)?\s+([0-9.]+)", log_text, re.IGNORECASE)
    if match_ssim:
        metrics["ssim"] = match_ssim.group(1) or match_ssim.group(2)
        
    # Entropy
    match_entropy = re.search(r"Entrop(?:y|ia)\s*[:=]?\s*([0-9.]+)", log_text, re.IGNORECASE)
    if match_entropy:
        metrics["entropy"] = match_entropy.group(1)
        
    # File Size
    if os.path.exists(output_bin):
        size_bytes = os.path.getsize(output_bin)
        metrics["size"] = f"{size_bytes / 1024:.2f} KB"
        
    return metrics
